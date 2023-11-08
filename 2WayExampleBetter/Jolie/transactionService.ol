from database import ConnectionInfo

type TransactionHandle: string
type TransactionResult: string

type ConnectRequest: ConnectionInfo
type ConnectResponse: string

type ExecuteQueryRequest{
    .handle: TransactionHandle
    .query: string
}

type QueryResult: any   //TODO: Figure out how to return the result of a query

interface TransactionServiceInterface {
    RequestResponse: 
        connect( ConnectRequest ) ( ConnectResponse ),
        startTransaction( void )( TransactionId ),
        executeQueryInTransaction( ExecuteQueryRequest )( QueryResult ),
        commitChanges( TransactionHandle )( TransactionResult )
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