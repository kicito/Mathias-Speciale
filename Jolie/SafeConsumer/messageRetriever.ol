include "console.iol"
include "time.iol"
include "database.iol"
include "inboxTypes.iol"
include "simpleConsumerInterface.iol"

from .simple-kafka-connector import KafkaConsumer

interface MessageRetrieverInterface{

}

service MessageRetriever(p: InboxEmbeddingConfig) {
    outputPort InboxPort {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxInterface            
    }
    embed KafkaConsumer as KafkaConsumer

    main
    {
        InboxPort.location << p.localLocation

        with ( pollOptions )
        {
            .pollAmount = 3;
            .pollDurationMS = 3000
        };

        with ( kafkaOptions )
        {
            .bootstrapServer =  "localhost:9092";
            .groupId = "test-group";
            .topic = "local-demo"
        };

        // Initialize Inbox Service
        with ( inboxSettings ){
            .pollOptions << pollOptions;
            .brokerOptions << kafkaOptions
        }

        Initialize@KafkaConsumer( inboxSettings )( initializedResponse )
        consumeRequest.timeoutMs = 3000
        while (true) {
            Consume@KafkaConsumer( consumeRequest )( consumeResponse )
            println@Console( "InboxService: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                println@Console( "InboxService: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                updateNumberRequest.userToUpdate = consumeResponse.messages[i].key
                recieveKafka@InboxPort( "New Message Recievied" )( simpleconsumerResponse )
                
                commitRequest.offset = consumeResponse.messages[i].offset
                Commit@KafkaConsumer( commitRequest )( commitResponse )
            }
            sleep@Time( 1000 )(  )
        }
    }
}