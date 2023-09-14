include "database.iol"

interface MessageRelayerInterface 
{
    OneWay: initialize ( void ) 
}

service MessageRelayer 
{
    execution: concurrent
    inputPort MessageRelayer {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: MessageRelayerInterface
    }

    init 
    {
        TABLENAME = "Messages"
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        };
        connect@Database( connectionInfo )( void );
    }

    main 
    {
        [ initialize() 
        {
            while (true)
            {
                // Get all from Messages tabel

                // Foreach message in row:
                    //  Forward message into Kafka, and wait for ack
                    //  When ack recieved, delete message from table
                //  Sleep 3000
            }
        }]
    }
}