include "database.iol"
include "console.iol"
include "time.iol"
from .simple-kafka-connector import SimpleKafkaConnector

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
    embed SimpleKafkaConnector as KafkaRelayer

    init
    {
        TABLENAME = "Numbers"
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
                "CREATE TABLE Numbers(username VARCHAR(50) NOT NULL, " +
                "number int)";
            update@Database( updateRequest )( ret )
        }
    }

    main {
        [ updateNumber( request )( response )
        {
            scope ( InsertData )    //Update the number in the database
            {   
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )
                updateQuery = "UPDATE " + TABLENAME + " SET number = number + 1 WHERE username = \"" + request.username + "\""
                update@Database( updateQuery )( updateResponse )
                println@Console( "Update response: " + updateResponse )(  )
            }
            // sleep@Time( 10000 )(  )      //Sleep time is in ms
            // If service crashes here, then the databases of the producer and consumer are inconsistant
            if ( updateResponse == 1)   // The update succedded
            {
                println@Console( "Forwarding message with username: " + request.username )()
                updateCountForUsername@KafkaRelayer( request.username )
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

