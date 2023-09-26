include "database.iol"
include "console.iol"
include "time.iol"

interface SimpleConsumerInterface {
    requestResponse: dbHasUpdated(void)(string)
}

service SimpleConsumer {
    execution: concurrent

    inputPort SimpleConsumer {
        location: "local"
        protocol: "http"
        interfaces: SimpleConsumerInterface
    }

    init {
        TABLENAME = "Numbers"
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
        
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
        [hello()(res) {
            res = "World"
        }]
	}

}
