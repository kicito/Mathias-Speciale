include "database.iol"
include "time.iol"
include "console.iol"
include "simpleConsumerInterface.iol"

from runtime import Runtime

service ServiceB{
    execution: sequential
    
    // Inputport used by the inbox
    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }
    embed Runtime as Runtime

    init {
        // Load the inbox service
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime( { 
            filepath = "inboxService.ol"
            params << { 
                localLocation << localLocation
                externalLocation << "socket://localhost:8082"
            }
        } )( _ )

        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        connect@Database( connectionInfo )( void )
        update@Database("CREATE TABLE IF NOT EXISTS example(message VARCHAR(100));")()

    }

    main {
        [updateNumberForUser( request )( response ){
            query@Database( "SELECT * FROM inbox WHERE hasBeenRead = false" )( inboxMessages )

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
                
                    updateQuery.topic = "service-b-local-updates"
                    updateQuery.key = "numbersUpdated"
                    updateQuery.value = username
                    transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                    println@Console("Service B has updated locally")()
                }
            }
            println@Console( "SimpleConsumer: Handling request for username " + request.userToUpdate )()
            // transactionalOutboxUpdate@OutboxService(Update table for user, 'Confirmed, i did it!')()
            response.code = 200
            response.reason = "Updated number locally"
        }]
    }
}