from .transactionService import TransactionService
include "console.iol"
include "database.iol"

type SayHelloRequest{
    .username: string
}

interface TestInterface {
    RequestResponse: sayHello( SayHelloRequest )( string )
}

service TestService{
    execution: concurrent
    inputPort ServiceALocal {
        location: "socket://localhost:8080" 
        protocol: http{
            format = "json"
        }
        interfaces: TestInterface
    }
    embed TransactionService as TransactionService
    
    init {
        // connect to DB
        with ( connectionInfo ) {
            .username = "postgres";
            .password = "example";
            .host = "";
            .database = "example-db"; // "." for memory-only
            .driver = "postgresql"
        }

        connect@Database(connectionInfo)()
        update@Database("CREATE TABLE IF NOT EXISTS Numbers(username VARCHAR(50) NOT NULL, number int)")()
        close@Database( )(  )
    }
    main
    {
        [sayHello( request )( response ){
            println@Console( "Jolie saying hello!" )(  )
            sayHello@TransactionService( request )( res )
            println@Console("Finished Jolie!")()
            response = "nice"
        }]
    }
}