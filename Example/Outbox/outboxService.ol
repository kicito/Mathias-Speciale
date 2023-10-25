include "time.iol"
include "console.iol"
include "database.iol"
from .messageForwarderService import MessageForwarderService

type KafkaOptions: void {   
    .topic: string                              // The topic to write updates to
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
}

type RabbitMqOptions {      // Not implemented
    .bootstrapServers: string
    .groupId: string
}

type PollSettings: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type UpdateOutboxRequest{
    .sqlQuery*: string                                   // The query that is to be executed against the database
    .key: string                                        // The key to use in the kafka message
    .value: string                                      // The value for the kafka message
    .topic: string                                      // The kafka topic on which the update should be broadcast
}

type ConnectOutboxRequest{
    .databaseConnectionInfo: ConnectionInfo             // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                         // Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .brokerOptions: KafkaOptions // RabbitMqOptions
}

type StatusResponse {
    .status: int
    .reason: string
}

interface OutboxInterface{
    RequestResponse:
        transactionalOutboxUpdate( UpdateOutboxRequest )( StatusResponse )
    OneWay:
        connectKafka( ConnectOutboxRequest ),
        connectRabbitMq( ConnectOutboxRequest )
}

/**
* This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
* It will then embeds a 'RelayService', which reads from the 'Messages' table and forwards messages into Kafka.
*/
service Outbox{
    execution: sequential
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }
    embed MessageForwarderService as RelayService

    init {
        println@Console("OutboxService initialized")()
    }
    
    main {
        [connectRabbitMq( request )]{
            response.status = 500
            response.reason = "Not implemented yet"

            global.M_MessageBroker = "RabbitMq"
        }

        /*
        * Connect to a Kafka version of this service.
        */
        [connectKafka( request ) ]{            
            println@Console("OutboxService: \tInitializing connection to Kafka")();
            connect@Database( request.databaseConnectionInfo )( void )
            scope ( createMessagesTable )
            {
                install ( SQLException => {
                    println@Console("Error when creating the outbox table for the outbox!")();
                    response.reason = "Error when creating the outbox table for the outbox!";
                    resposnse.code = 500
                    })

                // Varchar size is not enforced by sqlite, we can insert a string of any length
                updateRequest = "CREATE TABLE IF NOT EXISTS outbox (kafkaKey VARCHAR(50), kafkaValue VARCHAR (150), mid INTEGER PRIMARY KEY AUTOINCREMENT);"
                update@Database( updateRequest )( ret )
            }

            relayRequest.databaseConnectionInfo << request.databaseConnectionInfo
            relayRequest.pollSettings << request.pollSettings

            relayRequest.columnSettings.keyColumn = "kafkaKey"
            relayRequest.columnSettings.valueColumn = "kafkaValue"
            relayRequest.columnSettings.idColumn = "mid"
            relayRequest.brokerOptions << request.brokerOptions
            
            startReadingMessages@RelayService( relayRequest )

            global.M_MessageBroker = "Kafka"
        }


        /*
        *Executes the request.query as well as writing the request.message to the messages table
        */
        [transactionalOutboxUpdate( req )( res ){
            if (global.M_MessageBroker == "Kafka"){
                install (ConnectionError => response = "Call to update before connecting" )

                updateMessagesTableQuery = "INSERT INTO outbox (kafkaKey, kafkaValue) VALUES (\"" + req.key + "\", \"" + req.value + "\");" 
                transactionRequest.statement << request.sqlQuery

                transactionRequest.statement[#transactionRequest.statement] = updateMessagesTableQuery
                println@Console( "OutboxService initiating transactional update with queries " )( )
                println@Console("\t\t" + transactionRequest.statement[0] + "\n\t\t" + transactionRequest.statement[1] + "\n\t\t" + transactionRequest.statement[2])()

                executeTransaction@Database( transactionRequest )( transactionResponse )
                res.status = 200
                res.reason = "Transaction executed sucessfully"
            }
        }]
    }
}