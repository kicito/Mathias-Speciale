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
    }
    main
    {
        [sayHello( request )( response ){
            connect@TransactionService( connectionInfo )(  )
            initiate@TransactionService(  )( tHandle )
            executeQuery@TransactionService( "SELECT * FROM numbers;" )( qResp )
            foreach ( row : qResp.row ) {
                println@Console( "User: " + row.username + " has drunk " + row.number + " beers" )(  )
            }
            executeUpdate@TransactionService( "INSERT INTO numbers (username, number) VALUES ('HeyManTransaction', 1);" )( uResp )
            println@Console("How many rows were updated? Answer: " + uResp)()
            response = "nice"
        }]
    }
}