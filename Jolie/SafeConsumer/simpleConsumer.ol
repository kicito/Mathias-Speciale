include "database.iol"
include "time.iol"
include "console.iol"
include "simpleConsumerInterface.iol"

from runtime import Runtime

service SimpleConsumer{
    execution: sequential
    
    // Inputport used by the inbox
    inputPort InternalInputPort {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: SimpleConumerInterface
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
        } )( lol )

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

