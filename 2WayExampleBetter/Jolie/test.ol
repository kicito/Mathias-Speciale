from .transactionService import TransactionService
include "console.iol"

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