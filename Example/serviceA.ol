include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"

from .outboxService import Outbox

type UpdateNumberRequest {
    .username : string
}

type LocalUpdateResponse {
    .code: int
    .reason: string
}

type UpdateNumberResponse: string

interface ChoreographyInitializerInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse ),
        inboxUpdated( string ) ( LocalUpdateResponse ) 
}

service ServiceA{
    execution: concurrent

    // This port allows for a curl request to start the choreography
    //      curl http://localhost:8080/updateNumber?username=user1
    inputPort ServiceASocket {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyInitializerInterface
    }

    // This port allows for the inbox service to send updates to this service
    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyInitializerInterface
    }
    embed Outbox as OutboxService

    init
    {
        println@Console("Initializing main service")()

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

    main {
        [ updateNumber( request )( response )
        {
            println@Console("ServiceA: \tUpdateNumber called with username " + request.username)()
            scope ( InsertData )    //Update the number in the database
            {   
                //install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )

                connect@Database(config.serviceAConnectionInfo)()
                query@Database("SELECT * FROM Numbers WHERE username = \"" + request.username + "\"")( userExists )

                if (#userExists.row < 1){
                    updateQuery.sqlQuery = "INSERT INTO Numbers VALUES (\"" + request.username + "\", 0);"
                } else{
                    updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.username + "\""
                }
                updateQuery.topic = "service-a-local-updates"
                updateQuery.key = request.username
                updateQuery.value = "Updated number for " + request.username
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                response = "Choreography Started!"
            }
        }]

        [inboxUpdated( req )( res )
        {
            connect@Database(config.serviceAConnectionInfo)()
            checkInbox
            res.code = 1
            res.reason = "Choreography completed!"
        }]
    }
}

