include "console.iol"
include "time.iol"
include "file.iol"
include "database.iol"
include "inboxTypes.iol"

from .kafka-retriever import KafkaConsumer

service MessageRetriever(p: InboxEmbeddingConfig) {
    // This port is used to notify the inbox service of new messages
    outputPort InboxPort {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: InboxInterface            
    }
    embed KafkaConsumer as KafkaConsumer

    init
    {
        InboxPort.location << p.localLocation
        println@Console( "MessageRetriever Initialized" )(  )
    }

    main
    {
        readFile@File(
            {
                filename = p.configFile
                format = "json"
            }) ( config )


        with ( inboxSettings ){
            .pollOptions << config.pollOptions;
            .brokerOptions << config.kafkaInboxOptions
        }

        Initialize@KafkaConsumer( inboxSettings )( initializedResponse )
        consumeRequest.timeoutMs = 3000

        while (true) {
            Consume@KafkaConsumer( consumeRequest )( consumeResponse )
            println@Console( "InboxService: Received " + #consumeResponse.messages + " messages from KafkaConsumerService" )(  )

            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                println@Console( "InboxService: Retrieved message: " + consumeResponse.messages[i].value + " at offset " + consumeResponse.messages[i].offset)( )
                recievedKafkaMessage << consumeResponse.messages[i]
                RecieveKafka@InboxPort( recievedKafkaMessage )( recievedKafkaMessageResponse )
                if ( recievedKafkaMessageResponse == "Message stored" ||
                     recievedKafkaMessageResponse == "Message already recieveid, please re-commit" ){
                    commitRequest.offset = consumeResponse.messages[i].offset
                    Commit@KafkaConsumer( commitRequest )( commitResponse )
                }
            }
            sleep@Time( 1000 )(  )
        }
    }
}