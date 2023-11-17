include "database.iol"
include "console.iol"
include "time.iol"

from .outboxService import KafkaOptions
from .outboxService import PollSettings
from .outboxService import StatusResponse
from .kafka-inserter import KafkaInserter

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

interface MessageForwarderInterface {
    OneWay: startReadingMessages ( ForwarderServiceInfo )
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
    embed KafkaInserter as KafkaInserter

    /** Starts this service reading continually from the 'Messages' table */
    main{
        [startReadingMessages( request )] 
        {
            connect@Database( request.databaseConnectionInfo )( void )
            println@Console( "OutboxMessageForwarder Initialized" )(  )
            
            // Keep polling for messages at a given interval.
            while(true) {
                query = "SELECT * FROM outbox LIMIT " + request.pollSettings.pollAmount
                query@Database( query )( pulledMessages )
                if (#pulledMessages.row > 0){
                    println@Console( "OutboxMessageForwarder: \tForwarding " +  #pulledMessages.row + " messages into kafka!")(  )

                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = request.brokerOptions.topic
                        kafkaMessage.key = databaseMessage.kafkaKey
                        kafkaMessage.value = databaseMessage.kafkaValue
                        kafkaMessage.brokerOptions << request.brokerOptions

                        propagateMessage@KafkaInserter( kafkaMessage )( kafkaResponse )

                        println@Console( "Response success: " + kafkaResponse.success )(  )
                        if (kafkaResponse.success == true) {
                            deleteQuery = "DELETE FROM outbox WHERE  mid = " + databaseMessage.mid
                            println@Console( "OutboxMessageForwarder: \tExecuting query '" + deleteQuery + "'")(  )
                            update@Database( deleteQuery )( updateResponse )
                        }
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
    }
}