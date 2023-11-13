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
            with (qReqOne){
                .handle << tHandle;
                .query <<  "SELECT * FROM numbers;"
            }
            executeQuery@TransactionService(qReqOne )( qResp )
            for ( row in qResp.row ) {
                println@Console( "User: " + row.username + " has drunk " + row.number + " beers" )(  )
            }
            with (qReqTwo){
                .handle << tHandle;
                .update << "INSERT INTO numbers (username, number) VALUES ('HeyManTransaction', 1);" 
            }
            executeUpdate@TransactionService( qReqTwo )( uResp )
            println@Console("How many rows were updated? Answer: " + uResp)()
            commit@TransactionService( tHandle )( cResp )
            print@Console( "Response: " + cResp )(  )
            response = "nice"
        }]
    }
}