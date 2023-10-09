include "database.iol"
include "time.iol"
include "console.iol"

type UpdateDatabaseRequest{
    .userToUpdate: string
}

type UpdateDatabaseResponse {
    .code: int
    .reason: string
}

interface SimpleConumerInterface{
    RequestResponse:
        UpdateNumberForUser( UpdateDatabaseRequest )( UpdateDatabaseResponse )
}

service SimpleConsumer{
    execution{ sequential }
    inputPort SimpleConsumerSocket {
        location: "socket://localhost:8082" 
        protocol: http{
            format = "json"
        }
        interfaces: SimpleConumerInterface
    }

    inputPort SimpleConsumer {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: SimpleConumerInterface
    }

    init {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

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
            println@Console( "SimpleConsumer: Handling request for username " + request.userToUpdate )()
            // transactionalOutboxUpdate@OutboxService(Update table for user, 'Confirmed, i did it!')()
            response.code = 200
            response.reason = "Updated number locally"
        }]
    }
}

