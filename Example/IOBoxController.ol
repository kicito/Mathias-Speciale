include "simpleConsumerInterface.iol"
include "console.iol"
include "inboxTypes.iol"

from database import Database
from runtime import Runtime

interface IOBoxControllerInterface{
    OneWay: inboxTableUpdated( string )
    RequestResponse: transactionalOutboxUpdate( string )( string )
}

service IOBoxControllerService (p: InboxEmbeddingConfig)
{
    execution: concurrent
    inputPort InboxServiceNotifications 
    {
        location: "local" 
        protocol: http
        {
            format = "json"
        }
        interfaces: IOBoxControllerInterface
    }
    outputPort SimpleConsumer 
    {
        location: "local" 
        protocol: http
        {
            format = "json"
        }
        interfaces: SimpleConsumerInterface
    }
    embed Runtime as Runtime
    embed Database as Database

    init
    {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        SimpleConsumer.location = p.localLocation
        getLocalLocation@Runtime(  )( localLocation )
        loadEmbeddedService@Runtime( { 
            filepath = "inboxService.ol"
            params << { 
                localLocation << localLocation
                externalLocation << p.externalLocation
            }
        } )( lol )

        connect@Database( connectionInfo )( success )
    }

    cset 
    {
        transactionSession: UpdateDatabaseRequest.sid
    }

    main
    {
        [inboxTableUpdated( updatedUsername )]
        {
            transactionSession = new

            query@Database("SELECT * FROM inbox WHERE hasBeenRead = false AND username = \"" + updatedUsername + "\" LIMIT 1")( inboxRow )
            rowId = inboxRow.row[0].rowid

            global.openTransactions.( transactionSession ).queries[0] = "UPDATE inbox SET hasBeenRead = True WHERE rowid = " + rowId

            request.userToUpdate = updatedUsername
            updateNumberForUser@SimpleConsumer( request )( response )
            
            with ( updateStatements )
            {
                .statement << global.openTransactions.(transactionSession).queries
            }
             
            executeTransaction@Database( updateStatements )( transactionResponse )
        }

        [transactionalOutboxUpdate( request )( response ){
            if (global.M_MessageBroker == "Kafka"){
                numSessionQueries = #global.openTransactions.(transactionSession).queries
                global.openTransactions.(transactionSession).queries[numSessionQueries] = request
                response.reason = "Transaction executed sucessfully"
            }
        }]
    }
}