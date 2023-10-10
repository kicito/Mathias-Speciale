include "database.iol"
include "time.iol"
include "console.iol"

from .outboxService import Outbox

type UpdateDatabaseRequest{
    .userToUpdate: string
}

type UpdateDatabaseResponse {
    .code: int
    .reason: string
}

interface ChoreographyParticipantInterface {
    RequestResponse:
        UpdateNumberForUser( UpdateDatabaseRequest )( UpdateDatabaseResponse )
}

service ServiceB{
    execution{ sequential }

    // This port allows for calling the consumer locally
    inputPort ServiceBSocket {
        location: "socket://localhost:8082" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyParticipantInterface
    }

    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyParticipantInterface
    }
    embed Outbox as OutboxService


    init {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:serviceBDb.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
        
        with ( pollSettings )
        {
            .pollAmount = 3
            .pollDurationMS = 3000
        }

        with ( kafkaOptions )
        {
            .topic =  "service-b-local-updates"
            .bootstrapServers =  "kafka:9092"
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

        scope ( createtable ) 
        {
            install ( sqlexception => println@Console("numbers table already exists")() )
            updaterequest =
                "create table if not exists numbers(username varchar(50) not null, " +
                "number int)";
            update@Database( updaterequest )( ret )
        }
    }

    main {
        [UpdateNumberForUser( request )( response ){
            scope ( InsertData )    //Update the number in the database
            {   
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )
                updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.userToUpdate + "\""
                updateQuery.topic = "service-b-local-updates"
                updateQuery.key = request.userToUpdate
                updateQuery.value = "Updated number for " + request.userToUpdate
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                println@Console("Service B has updated locally")()
                response.code = 200
                response.reason = "Updated number locally"
            }
            
        }]
    }
}

