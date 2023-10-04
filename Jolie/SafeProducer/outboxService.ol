
include "outboxServiceInterface.iol"

from .messageforwarder import MessageForwarderService

service Outbox{
    execution: sequential
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }
    embed MessageForwarderService as RelayService

    init {
        MESSAGES_TABLE_KEY_COLUMN = "kafkaKey"
        MESSAGES_TABLE_VALUE_COLUMN = "kafkaValue"
        MESSAGES_TABLE_ID_COLUMN = "mid"
    }

    main {
        [connect( request )]{
            connect@Database( request.databaseConnectionInfo )( void )
            scope ( createMessagesTable )
            {
                install ( SQLException => println@Console("Error when creating the messages table for the outbox!")() );
                // Varchar size is not enforced by sqlite, we can insert a string of any length
                updateRequest = "CREATE TABLE IF NOT EXISTS messages (" + 
                    MESSAGES_TABLE_KEY_COLUMN + " VARCHAR(50), " +
                    MESSAGES_TABLE_VALUE_COLUMN + " VARCHAR (150), " +
                    MESSAGES_TABLE_ID_COLUMN + " INTEGER PRIMARY KEY AUTOINCREMENT);"
                update@Database( updateRequest )( ret )
            }

            relayRequest.databaseConnectionInfo << request.databaseConnectionInfo
            relayRequest.pollSettings << request.pollSettings

            relayRequest.columnSettings.keyColumn = MESSAGES_TABLE_KEY_COLUMN
            relayRequest.columnSettings.valueColumn = MESSAGES_TABLE_VALUE_COLUMN
            relayRequest.columnSettings.idColumn = MESSAGES_TABLE_ID_COLUMN

            startReadingMessages@RelayService(relayRequest)
        }

        [transactionalOutboxUpdate( request )( response ){
            println@Console( "Initiating transactional update" )(  )
            install (ConnectionError => 
                {response = "Call to update before connecting"}
            )
            if (response != "Call to update before connecting"){
                println@Console("Connection established. Updating tables.")()

                updateMessagesTableQuery = "INSERT INTO messages (kafkaKey, kafkaValue) VALUES (\"" + request.key + "\", \"" + request.value + "\");" 
                transactionRequest.statement[0] = updateMessagesTableQuery
                transactionRequest.statement[1] = request.sqlQuery
                executeTransaction@Database( transactionRequest )( transactionResponse )
                response = "YAY"
            }
        }]
    }
}