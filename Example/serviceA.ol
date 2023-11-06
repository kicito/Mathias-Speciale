include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"
include "string_utils.iol" 
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
                configFile = "serviceAConfig.json"
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
            inboxDeleteQuery = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = " + row.kafkaOffset
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
            scope ( UpdateInboxTable )
            {
                install( SQLException => println@Console("HERE0")() )
                connect@Database(config.serviceAConnectionInfo)()
                query@Database("SELECT * FROM inbox WHERE hasBeenRead = false LIMIT 1")( inboxRow )

                inboxRow.row[0].request.regex = ":"
                split@StringUtils( inboxRow.row[0].request )( splitResult )
                username = splitResult.result[1]
                username.rowId = inboxRow.row[0].rowid
            }

            scope ( InsertData )    //Update the number in the database
            {   
                install ( SQLException => println@Console( "SQL exception occured in Service A while inserting data" )( ) )

                connect@Database(config.serviceAConnectionInfo)()
                query@Database("SELECT * FROM Numbers WHERE username = \"" + username + "\"")( userExists )

                if (#userExists.row < 1){
                    updateQuery.sqlQuery[0] = "INSERT INTO Numbers VALUES (\"" + username + "\", 0);"
                } else{
                    updateQuery.sqlQuery[0] = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + username + "\""
                }
                updateQuery.sqlQuery[1] = "UPDATE inbox SET hasBeenRead = true WHERE hasBeenRead = false AND rowid =" + username.rowId

                println@Console("\n Query: " + updateQuery.sqlQuery[1] + "\n")()

                updateQuery.topic = config.kafkaOutboxOptions.topic
                updateQuery.key = "updateNumber"
                updateQuery.value = username
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                res = "Choreography Started!"
            }
        }]

        [finalizeChoreography( req )]{
            scope ( lol ){
                install( SQLException => println@Console("Error is here!")() )
                connect@Database( config.serviceAConnectionInfo )()
                println@Console("Query: UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = " + req)()
                update@Database("UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = " + req)()
                //TODO: Again, the above query does not consider that the offset is NULL when message is not recieved from Kafka.
                    //  Again, it doesn't matter in this case, since this operation is only exposed to a local channel
                
                println@Console("Finished choreography")()

            }
        }
    }
}