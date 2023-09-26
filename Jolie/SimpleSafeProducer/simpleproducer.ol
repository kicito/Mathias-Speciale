
include "database.iol"
include "console.iol"
include "time.iol"

from .outboxservice import Outbox


type UpdateNumberRequest {
    .username : string
}

type UpdateNumberResponse: string

interface SimpleProducerInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
}

service SimpleProducer{
    execution: concurrent
    inputPort SimpleProducer {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: SimpleProducerInterface
    }
    embed Outbox as OutboxService

    init
    {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        };

        connect@OutboxService( connectionInfo );
        connect@Database( connectionInfo )(void);

        scope ( createTable ) 
        {
            install ( SQLException => println@Console("Numbers table already exists")() );
            updateRequest =
                "CREATE TABLE Numbers(username VARCHAR(50) NOT NULL, " +
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
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) );
                updateQuery.sqlQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.username + "\"";
                updateQuery.key = "Update";
                updateQuery.value = "Updated number for " + request.username;
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                println@Console( "Update response: " + updateResponse )(  )
                response = "Yay2"
            }
        }]
    }
}

