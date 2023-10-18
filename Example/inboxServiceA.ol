include "console.iol"
include "time.iol"
include "database.iol"
include "file.iol"

from .kafka-retriever import KafkaConsumer
from .serviceA import ServiceA

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
    embed ServiceA as ServiceA
    embed KafkaConsumer as KafkaConsumer

    main
    {
        readFile@File(
            {
                filename = "serviceAConfig.json"
                format = "json"
            }) ( config )

        with ( inboxSettings ){
            .pollOptions << config.pollOptions;
            .brokerOptions << config.kafkaInboxOptions
        }

        scope ( createtable ) 
        {
            connect@Database(config.serviceAConnectionInfo)()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (kafkaKey VARCHAR(50), kafkaValue VARCHAR (150), kafkaOffset INTEGER PRIMARY KEY AUTOINCREMENT);" )( ret )
        }

        Initialize@KafkaConsumer( inboxSettings )( initializedResponse )
        consumeRequest.timeoutMs = 3000

        while (true) {
            Consume@KafkaConsumer( consumeRequest )( consumeResponse )

            i = 0
            while (i < #consumeResponse.messages) {
                println@Console( "InboxService: \tRecieved a message on topic " + consumeResponse.messages[i].topic + " at offset: " + consumeResponse.messages[i].offset )()

                
                scope ( MakeIdempotent ){
                    // Write the message to the inbox table
                    install( SQLException => {
                        println@Console("InboxService: \tTrying to insert message into inbox twice!")()
                        commitRequest.offset = consumeResponse.messages[i].offset
                        Commit@KafkaConsumer( commitRequest )( commitResponse )
                        i++
                    })

                    key = consumeResponse.messages[i].key
                    value = consumeResponse.messages[i].value
                    offset = consumeResponse.messages[i].offset

                    update@Database( "INSERT INTO inbox VALUES (\"" + key + "\", \"" + value + "\", " + offset + ")" )( inboxResponse )
                    
                    if ( inboxResponse == 1 ){
                        // If the message was written correctly, tell kafka that we've read the message
                        commitRequest.offset = i
                        Commit@KafkaConsumer( commitRequest )( commitResponse )
                        println@Console( "InboxService: \t Message inserted correctly into table" )( )
                    } else {
                        // Break out of the 'for' loop, under the assumption that the database is unresponsive
                        i = #consumeResponse.messages
                    }

                    if ( commitResponse.status == 1 ){
                        // When Kafka acknowledges that it has moved its offset, tell main service that a new message is waiting
                        inboxUpdated@ServiceA( "Inbox Updated! Please check it :D" )( inboxUpdateResponse )
                    } else {
                        // Break inner loop under the assumption that Kafka is unresponsive
                        i = #consumeResponse.messages
                    }
                    i++
                }
            }
            sleep@Time( 1000 )(  )
        }
    }
}