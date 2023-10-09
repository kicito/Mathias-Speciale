include "console.iol"
include "time.iol"
include "database.iol"


from .simple-kafka-connector import KafkaConsumer
from .simpleConsumer import SimpleConsumer

type KafkaOptions: void {   
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
    .topic: string
}

type RabbitMqOptions {      // Not implemented
    .bootstrapServers: string
    .groupId: string
}

type PollOptions: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type InitializeConsumerRequest{
    .pollOptions: PollOptions
    .brokerOptions: KafkaOptions
}

interface InboxInterface{
}

service Inbox{
    inputPort InboxPort {
        Location: "local"
        Interfaces: InboxInterface
    }
    embed SimpleConsumer as SimpleConsumer
    embed KafkaConsumer as KafkaConsumer

    main
    {
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
                UpdateNumberForUser@SimpleConsumer( updateNumberRequest )( simpleconsumerResponse )
                if ( simpleconsumerResponse.code == 200 ){
                    commitRequest.offset = consumeResponse.messages[i].offset
                    Commit@KafkaConsumer( commitRequest )( commitResponse )
                }
            }
            sleep@Time( 1000 )(  )
        }
    }
}