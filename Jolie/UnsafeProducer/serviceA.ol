include "database.iol"
include "console.iol"
include "time.iol"
from .simple-kafka-connector import SimpleKafkaConnector

type UpdateNumberRequest {
    .username : string
}

type UpdateNumberResponse: string

interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
}

service ServiceA{
    execution: concurrent
    inputPort ServiceAExternal {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }
    embed SimpleKafkaConnector as KafkaRelayer

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
        
        connect@Database( connectionInfo )( void );
        scope ( createTable ) 
        {
            install ( SQLException => println@Console("Table already exists")() );
            updateRequest =
                "CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
            update@Database( updateRequest )( ret )
        }
    }

    main {
        [ updateNumber( request )( response )
        {
            updateQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + request.username + "\""
            // 1: Service A updates its local state
            update@Database( updateQuery )( updateResponse )

            /***** If service crashes here, then the databases of the producer and consumer are inconsistant *****/
            
            if ( updateResponse == 1)
            {
                // 3: Propagate the updated username into Kafka
                propagateMessage@KafkaRelayer( request.username )
                response = "Update succeded!"
            }
            else 
            {
                println@Console( "Forwarding message failed" )()
                response = "Updating failed!"
            }
            
        }]
    }
}

