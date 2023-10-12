include "console.iol"
include "database.iol"
include "time.iol"

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

        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        with ( kafkaOptions )
        {
            .bootstrapServers =  "localhost:9092";
            .groupId = "service-b-inbox";
            .topic = "service-a-local-updates"
        };

        // Initialize Inbox Service
        with ( inboxSettings ){
            .pollOptions << pollOptions;
            .brokerOptions << kafkaOptions
        }

        connect@Database( connectionInfo )( void )

        scope ( createtable ) 
        {
            install ( sqlexception => println@Console("inbox table already exists")() )
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (kafkaKey VARCHAR(50), kafkaValue VARCHAR (150), kafkaOffset INTEGER PRIMARY KEY AUTOINCREMENT);" )( ret )
        }

        Initialize@KafkaConsumer( inboxSettings )( initializedResponse )
        consumeRequest.timeoutMs = 3000

        while (true) {
            Consume@KafkaConsumer( consumeRequest )( consumeResponse )

            for ( i = 0, i < #consumeResponse.messages, i++ ) {
                scope ( MakeIdempotent ){
                    install( SQLException => println@Console("Trying to insert message into inbox twice!")() )
                    println@Console( "Recieved a message from kafka! Forwarding to main service!" )()
                    inboxUpdate.key = consumeResponse.messages[i].key
                    inboxUpdate.value = consumeResponse.messages[i].value
                    inboxUpdate.offset = consumeResponse.messages[i].offset
                    update@Database( "INSERT INTO inbox VALUES (\"" + inboxUpdate.key + "\", \"" + inboxUpdate.value + "\", " + inboxUpdate.offset + ")" )( inboxResponse )

                    inboxUpdateRequest.databaseConnectionInfo << connectionInfo
                    inboxUpdated@ServiceB( inboxUpdateRequest )( inboxUpdateResponse )
                    if ( simpleconsumerResponse.amountMessagesRead > 0 ){
                        //Commit@KafkaConsumer( commitRequest )( commitResponse )
                        println@Console( "Sucessfully forwarded to main service, which processed " + simpleconsumerResponse.amountMessagesRead + " messages.")()
                    }
                }
                
            }
        }
    }
}