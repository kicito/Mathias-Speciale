include "console.iol"
include "time.iol"
include "database.iol"

from .kafka-retriever import KafkaConsumer
from .serviceB import ServiceB

type KafkaOptions: void {   
    .bootstrapServers: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
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
    embed ServiceB as ServiceB
    embed KafkaConsumer as KafkaConsumer

    main
    {
        println@Console( "Initializing inboxservice" )()
        with ( pollOptions )
        {
            .pollAmount = 3;
            .pollDurationMS = 3000
        };

        with ( kafkaOptions )
        {
            .bootstrapServers =  "kafka:9092";
            .groupId = "service-b-inbox";
            .topic = "service-a-local-updates"
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

            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                println@Console( "Recieved a message from kafka! Forwarding to main service!" )()
                updateNumberRequest.userToUpdate = consumeResponse.messages[i].key
                updateNumberForUser@ServiceB( updateNumberRequest )( simpleconsumerResponse )
                if ( simpleconsumerResponse.code == 200 ){
                    commitRequest.offset = consumeResponse.messages[i].offset
                    Commit@KafkaConsumer( commitRequest )( commitResponse )
                    println@Console( "Sucessfully forwarded service. Commited offset: " + commitRequest.offset )()
                }
            }
            sleep@Time( 1000 )(  )
        }
    }
}