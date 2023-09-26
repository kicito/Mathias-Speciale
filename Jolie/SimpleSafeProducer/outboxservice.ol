include "database.iol"
include "console.iol"
include "time.iol"

from .relayservice import RelayService

type updateOutboxRequest{
    .sqlQuery : string
    //.topic : string
    .key : string
    .value : string
}

type updateOutboxResponse: string

interface OutboxInterface{
    OneWay:
        connect( ConnectionInfo )   // ConnectionInfo is defined by the database.iol library
    RequestResponse:
        transactionalOutboxUpdate( updateOutboxRequest )( updateOutboxResponse )

}

service Outbox{
    execution: sequential
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }
    embed RelayService as RelayService

    main {
        [connect( request )]{
            install ( SQLException => println@Console( "Messages table already exists" )( ) );
            connect@Database( request )( void )
            updateRequest = "CREATE TABLE messages(key VARCHAR(50) NOT NULL, value VARCHAR(150), timestamp int)"
            update@Database( updateRequest )( ret )

        }

        [transactionalOutboxUpdate( request )( response ){
            println@Console( "Initiating transactional update" )(  )
            install (ConnectionError => 
                {response = "Call to update before connecting"}
            )
            if (response != "Call to update before connecting"){
                println@Console("Connection established. Updating tables.")()

                getCurrentTimeMillis@Time()( time )

                updateMessagesTableQuery = "INSERT INTO messages VALUES (\"" + request.key + "\", \"" + request.value + "\", " + time + ");" 
                transactionRequest.statement[0] = updateMessagesTableQuery
                transactionRequest.statement[1] = request.sqlQuery
                executeTransaction@Database( transactionRequest )( transactionResponse )
                response = "YAY"
            }

        }]
    }
}