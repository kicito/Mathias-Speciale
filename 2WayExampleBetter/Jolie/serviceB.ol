from runtime import Runtime
from file import File
from database import Database
from console import Console
from string_utils import StringUtils
from .serviceBInterface import ServiceBInterface
from .Outbox.outboxService import Outbox
from .Inbox.inboxServiceB import Inbox

service ServiceB{
    execution: concurrent

    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    // TODO: Embed the outbox service dynamically to avoid having to call ConnectKafka to it
    embed Outbox as OutboxService
    // inbox service is loaded dynamically in the 'init' function
    embed Runtime as Runtime
    embed File as File
    embed Database as Database
    embed Console as Console
    embed StringUtils as StringUtils
    
    init {
        getLocalLocation@Runtime()( location )
        loadEmbeddedService@Runtime( { 
            filepath = "Inbox/inboxServiceB.ol"
            service = "Inbox"
            params << { 
                localLocation << location
                externalLocation << "socket://localhost:8082"       //This doesn't work
                configFile = "serviceBConfig.json"
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
    }

    main 
    {
        [numbersUpdated( request )]
        {
            scope (ForceSequentialDatabaseAccess)
            {
                connect@Database( config.serviceBConnectionInfo )( void )
                query@Database( "SELECT * FROM inbox WHERE hasBeenRead = false" )( inboxMessages )
            }

            for ( row in inboxMessages.row ) 
            {
                println@Console("ServiceB: Checking inbox found update request " + row.request)()
                row.request.regex = ":"
                split@StringUtils( row.request )( splitResult )
                username = splitResult.result[1]
                query@Database("SELECT * FROM Numbers WHERE username = \"" + username + "\"")( userExists )
                
                // Construct query which updates local state:
                if (#userExists.row < 1)
                {
                    localUpdateQuery = "INSERT INTO Numbers VALUES (\"" + username + "\", 0);"
                } 
                else
                {
                    localUpdateQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + username + "\""
                }

                // Construct query to delete message row from inbox table
                inboxDeleteQuery = "UPDATE inbox SET hasBeenRead = true WHERE rowid = " + row.rowid
                //TODO: The above query does not consider that kafkaOffset can be NULL for messages not coming out of Kafka.
                        // In this case, it doesn't matter, since all messages come from kafka, but in a more advanced example
                        // it might be a problem

                scope (ExecuteQueries)
                {
                    updateQuery.sqlQuery[0] = localUpdateQuery
                    updateQuery.sqlQuery[1] = inboxDeleteQuery
                
                    updateQuery.topic = config.kafkaOutboxOptions.topic
                    updateQuery.key = "numbersUpdated"
                    updateQuery.value = username
                    transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                    println@Console("Service B has updated locally")()
                }
            }
        }
    }
}