include "database.iol"
include "console.iol"
include "time.iol"

from .outboxService import KafkaOptions
from .outboxService import PollSettings
from .outboxService import StatusResponse
from .kafka-inserter import SimpleKafkaConnector

type ColumnSettings {
    .keyColumn: string
    .valueColumn: string
    .idColumn: string
}

type ForwarderServiceInfo {
    .databaseConnectionInfo: ConnectionInfo     // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                 // The settings to use
    .columnSettings: ColumnSettings            // The names of the columns in the 'messages' table
    .brokerOptions: KafkaOptions
}

type ForwarderResponse{
    .status: int
    .reason: string
}

interface MessageForwarderInterface {
    RequestResponse: startReadingMessages ( ForwarderServiceInfo ) ( StatusResponse )
}

/**
* This service is responsible for reading messages from the 'Messages' table, forwarding them to Kafaka, 
* then deleting the messages from the table when an ack is recieved fom Kafka
*/
service MessageForwarderService{
    execution{ sequential }
    inputPort Input {
        Location: "local"
        Interfaces: MessageForwarderInterface
    }
    embed SimpleKafkaConnector as KafkaRelayer

    /** Starts this service reading continually from the 'Messages' table */
    main{
        [startReadingMessages( request )( response ) 
        {
            scope (ConnectToDatabase){      // Everything in this scope is simply to check that a connection to the database CAN be opened.
                install (ConnectionError => 
                {
                    response.status = 500
                    response.reason = "MessageForwarderService could not connect to the database"
                    println@Console( "MessageForwarderService could not connect to the database" )(  )

                })
                global.M_KafkaOptions << request.brokerOptions
                response.status = 200
                response.reason = "MessageForwarderService initialized sucessfully"
                println@Console( "MessageForwarder connected to database!" )(  )
            }

        }] 
        {
            connect@Database( request.databaseConnectionInfo )( void )  // I very much hate that i have to do this again, and i CANNOT simply keep the connection from above open
            
            // Keep polling for messages at a given interval.
            while(true) {
                query = "SELECT * FROM messages LIMIT " + request.pollSettings.pollAmount
                query@Database(query)( pulledMessages )
                if (#pulledMessages.row > 0){
                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = request.brokerOptions.topic
                        kafkaMessage.key = databaseMessage.(request.columnSettings.keyColumn)
                        kafkaMessage.value = databaseMessage.(request.columnSettings.valueColumn)
                        kafkaMessage.brokerOptions << global.M_KafkaOptions
                        propagateMessage@KafkaRelayer( kafkaMessage )( kafkaResponse )
                        if (kafkaResponse.status == 200) {
                            update@Database( "DELETE FROM messages WHERE " + ( request.columnSettings.idColumn ) + " = " + databaseMessage.(request.columnSettings.idColumn) )( updateResponse )
                        }
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
    }
}