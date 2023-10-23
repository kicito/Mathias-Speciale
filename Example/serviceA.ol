include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"
include "serviceAInterface.iol"

from runtime import Runtime
from .Outbox.outboxService import Outbox
from .Inbox.inboxServiceA import Inbox

service ServiceA{
    execution: concurrent

    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    embed Runtime as Runtime
    embed Outbox as OutboxService

    init
    {
        getLocalLocation@Runtime()( location )
        loadEmbeddedService@Runtime( { 
            filepath = "Inbox/inboxServiceA.ol"
            params << { 
                localLocation << location
                externalLocation << "socket://localhost:8080"       //This doesn't work
            }
        } )( inbox.location )

        readFile@File(
            {
                filename = "serviceAConfig.json"
                format = "json"
            }) ( config )

        with ( outboxSettings )
        {
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceAConnectionInfo;
            .brokerOptions << config.kafkaOutboxOptions
        }
        
        connectKafka@OutboxService( outboxSettings )
        connect@Database( config.serviceAConnectionInfo )( void )

        scope ( createTable ) 
        {
            install ( SQLException => println@Console("Numbers table already exists")() )
            updateRequest =
                "CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
            update@Database( updateRequest )( ret )
        }
    }

    define checkInbox{
        query@Database( "SELECT * FROM inbox" )( inboxMessages )
        for ( row in inboxMessages.row ) {
            inboxDeleteQuery = "DELETE FROM inbox WHERE kafkaOffset = " + row.kafkaOffset
            println@Console("ServiceA: Choreography completed! Deleting inbox message for offset " + row.kafkaOffset)()
            update@Database(inboxDeleteQuery)()
        }
    }

    //TODO: A third service should really be running, looking for updates to the inbox table and
    //      forwarding these to the 'main' service. This would cause no messages to be skipped

    main {
        [ updateNumber( req )( res )
        {
            println@Console("ServiceA: \tUpdateNumber called with username " + req.username)()
            scope ( InsertData )    //Update the number in the database
            {   
                //install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )

                connect@Database(config.serviceAConnectionInfo)()
                query@Database("SELECT * FROM Numbers WHERE username = \"" + req.username + "\"")( userExists )

                if (#userExists.row < 1){
                    updateQuery.sqlQuery = "INSERT INTO Numbers VALUES (\"" + req.username + "\", 0);"
                } else{
                    updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + req.username + "\""
                }
                updateQuery.topic = "service-a-local-updates"
                updateQuery.key = "updateNumber"
                updateQuery.value = req.username
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                res = "Choreography Started!"
            }
        }]

        [finalizeChoreography( req )]{
            connect@Database(config.serviceAConnectionInfo)()
            query@Database("UPDATE inbox SET hasBeenRead = true WHERE offset = " req)()
            //TODO: Again, the above query does not consider that the offset is NULL when message is not recieved from Kafka.
                //  Again, it doesn't matter in this case, since this operation is only exposed to a local channel
            
            println@Console("Finished choreography")()
        }
    }
}

