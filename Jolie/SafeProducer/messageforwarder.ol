include "database.iol"
include "console.iol"
include "time.iol"

from .simple-kafka-connector import SimpleKafkaConnector

type PollSettings{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type ColumnSettings {
    .keyColumn: string
    .valueColumn: string
    .idColumn: string
}

type ForwarderServiceInfo {
    .databaseConnectionInfo: ConnectionInfo     // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                 // The settings to use
    .columnSettings: ColumnSettings            // The names of the columns in the 'messages' table
}

interface  MessageForwarderInterface {
    OneWay: startReadingMessages ( ForwarderServiceInfo )
}

service MessageForwarderService{
    inputPort Input {
        Location: "local"
        Interfaces: MessageForwarderInterface
        }
    embed SimpleKafkaConnector as KafkaRelayer

    main{
        [startReadingMessages( request )] {
            connect@Database( request.databaseConnectionInfo )( void )
            while(true) {
                query = "SELECT * FROM messages LIMIT " + request.pollSettings.pollAmount
                println@Console( "Query: " + query)(  )
                query@Database(query)( pulledMessages )
                println@Console( "Query '" + query + "' returned " + #pulledMessages.row + " rows " )(  )
                if (#pulledMessages.row > 0){
                    for ( databaseMessage in pulledMessages.row ){
                        kafkaMessage.topic = "local-demo"
                        kafkaMessage.key = databaseMessage.(request.columnSettings.keyColumn)
                        kafkaMessage.value = databaseMessage.(request.columnSettings.valueColumn)
                        propagateMessage@KafkaRelayer(kafkaMessage)
                        update@Database( "DELETE FROM messages WHERE " + (request.columnSettings.idColumn) + " = " + databaseMessage.(request.columnSettings.idColumn) )( updateResponse )
                    }
                }
                sleep@Time( request.pollSettings.pollDurationMS )(  )
            }
        }
    }
}