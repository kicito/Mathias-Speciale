include "database.iol"
include "console.iol"

from .simple-kafka-connector import SimpleKafkaConnector

type RelayInformation{
    .databaseConnectionInfo: ConnectionInfo
    .topic: string
    .pollAmouunt: int
    .pollDuration: int
    //.kafkaSettings: KafkaSettings
}

interface  RelayServiceInterface {
    OneWay: startReadingMessages ( ConnectionInfo )
}

service RelayService{
    inputPort Input {
        Location: "local"
        Interfaces: RelayServiceInterface
        }
    embed SimpleKafkaConnector as KafkaRelayer

    main{
        [startReadingMessages( request )] {
            println@Console("Hey, i'm here 1!")();
            connect@Database( request.databaseConnectionInfo )( void )
            while(true) {
                query@Database("SELECT * FROM messages WHERE ROWID <= " + pollAmouunt)( pulledMessages )
                if (#pulledMessages.rows > 0){
                    println@Console("Hey, i'm here 2!")();
                    message.topic = "local-demo"
                    message.key = pulledMessages.rows[0].key
                    message.value = pulledMessages.rows[0].value
                    propagateMessage@KafkaRelayer(message)
                }
            }
        }
    }
}