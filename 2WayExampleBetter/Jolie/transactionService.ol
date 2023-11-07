type SayHelloRequest{
    .username: string
}

type SayHelloResponse {
    .response: string
}

interface TransactionServiceInterface {
    RequestResponse: 
        sayHello( SayHelloRequest ) ( SayHelloResponse ),
}

service TransactionService{
    inputPort Input {
        Location: "local"
        Interfaces: TransactionServiceInterface
        } 
        foreign java {
            class: "jolie.transactionservice.TransactionService"
        }
}