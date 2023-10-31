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
        interfaces: SimpleConsumerInterface
    }
    embed Runtime as Runtime

    init {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        // Load the inbox service
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime( { 
            filepath = "IOBoxController.ol"
            params << { 
                localLocation << localLocation
                externalLocation << "socket://localhost:8082"
            }
        } )( lol )

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
        [updateNumberForUser( request )( response ){
            println@Console( "SimpleConsumer: Handling request for username " + request.userToUpdate )()
            update@Database( "UPDATE table SET number = number + 1 WHERE username = \"" + request.userToUpdate + "\"" )()
            
            
            response.reason = "Updated number locally"
        }]
    }
}

