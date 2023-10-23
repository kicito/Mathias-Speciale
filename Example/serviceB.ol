include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"
include "runtime.iol"

from runtime import Runtime
from .outboxService import Outbox
from .Inbox.inboxServiceB import Inbox

service ServiceB{
    execution: concurrent

    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyParticipantInterface
    }

    embed Outbox as OutboxService
    embed Runtime as Runtime

    init {
        getLocalLocation@Runtime()( location )
        loadEmbeddedService@Runtime( { 
            filepath = "Inbox/inboxServiceB.ol"
            params << { 
                localLocation << location
                externalLocation << "socket://localhost:8082"       //This doesn't work
            }
        } )( inbox.location )

        readFile@File(
            {
                filename = "serviceBConfig.json"
                format = "json"
            }) ( config )

        with ( outboxSettings )
        {
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceBConnectionInfo;
            .brokerOptions << config.kafkaOutboxOptions
        }

        connectKafka@OutboxService( outboxSettings )
        connect@Database( config.serviceBConnectionInfo )( void )

        scope ( createtable ) 
        {
            updaterequest = "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)"
            update@Database( updaterequest )( ret )
        }
        checkInbox
    }

    // This procedure requires an existing connection to a DB
    define checkInbox{
        scope (ForceSequentialDatabaseAccess){
            install( SQLException => checkInbox )
            query@Database( "SELECT * FROM inbox WHERE hasBeenRead = false" )( inboxMessages )
        }

        for ( row in inboxMessages.row ) {
            println@Console("ServiceB: Checking inbox found update request for " + row.kafkaValue)()
            query@Database("SELECT * FROM Numbers WHERE username = \"" + row.kafkaValue + "\"")( userExists )
            
            // Construct query which updates local state:
            if (#userExists.row < 1){
                    localUpdateQuery = "INSERT INTO Numbers VALUES (\"" + row.kafkaValue + "\", 0);"
                } else{
                    localUpdateQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + row.kafkaValue + "\""
                }

            // Construct query to delete message row from inbox table
            inboxDeleteQuery = "UPDATE inbox SET hasBeenRead = true WHERE kafkaOffset = " + row.kafkaOffset
            //TODO: The above query does not consider that kafkaOffset can be NULL for messages not coming out of Kafka.
                    // In this case, it doesn't matter, since all messages come from kafka, but in a more advanced example
                    // it might be a problem

            scope (ExecuteQueries){
                updateQuery.sqlQuery[0] = localUpdateQuery
                updateQuery.sqlQuery[1] = inboxDeleteQuery
            
                updateQuery.topic = "service-b-local-updates"
                updateQuery.key = "numbersUpdated"
                updateQuery.value = row.kafkaValue
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                println@Console("Service B has updated locally")()
            }
        }
        response.amountMessagesRead = #inboxMessages.row
    }

    main {
        [numbersUpdated( request )( response ){
            connect@Database(config.serviceBConnectionInfo)()
            checkInbox
        }]
    }
}

