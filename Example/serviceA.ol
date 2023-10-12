include "database.iol"
include "console.iol"
include "time.iol"

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
        numberCorrectlyUpdated( string ) ( LocalUpdateResponse ) 
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
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:serviceADb.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
        
        with ( pollSettings )
        {
            .pollAmount = 3
            .pollDurationMS = 3000
        }

        with ( kafkaOptions )
        {
            .topic =  "service-a-local-updates"
            .bootstrapServers = "localhost:9092"
            .groupId = "test-group"
        }

        with ( outboxSettings )
        {
            .pollSettings << pollSettings;
            .databaseConnectionInfo << connectionInfo;
            .brokerOptions << kafkaOptions
        }
        
        connectKafka@OutboxService( outboxSettings ) ( outboxResponse )
        connect@Database( connectionInfo )( void )

        scope ( createTable ) 
        {
            install ( SQLException => println@Console("Numbers table already exists")() )
            updateRequest =
                "CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
            update@Database( updateRequest )( ret )
        }
    }

    main {
        [ updateNumber( request )( response )
        {
            println@Console("UpdateNumber called with username " + request.username)()
            scope ( InsertData )    //Update the number in the database
            {   
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )
                updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.username + "\""
                updateQuery.topic = "service-a-local-updates"
                updateQuery.key = request.username
                updateQuery.value = "Updated number for " + request.username
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                response = "Choreography Started!"
            }
        }]

        [numberCorrectlyUpdated( req )( res )
        {
            println@Console("Choreography completed!")()
            res.code = 200
            res.reason = "Choreography completed!"
        }]
    }
}

