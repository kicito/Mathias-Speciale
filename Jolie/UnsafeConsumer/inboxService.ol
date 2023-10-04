include "console.iol"
include "time.iol"

from .simple-kafka-connector import SimpleKafkaConsumer

interface InboxInterface{
    OneWay:
        Initialize( string )
}

service Inbox{
    execution: sequential
    inputPort InboxPort {
        Location: "local"
        Interfaces: InboxInterface
    }
    embed SimpleKafkaConsumer as KafkaConsumer

    main {
        [Initialize()] {
            Initialize@KafkaConsumer("Hi");
            while (true) {
                Consume@KafkaConsumer("Consuming")( consumerMessage )
                if (#consumerMessage.messages > 0){
                    println@Console( consumerMessage.code )(  )
                    for ( i = 0, i < #consumerMessage.messages, i++ ) {
                        println@Console(consumerMessage.messages[i])()
                    }
                }
            }
        }
    }
}