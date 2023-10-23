include "console.iol"
include "time.iol"
include "database.iol"
include "inboxTypes.iol"
include "simpleConsumerInterface.iol"

from runtime import Runtime
service Inbox(p: InboxEmbeddingConfig){
    execution: concurrent

    // This service takes over handling of the external endpoint from the embedder
    inputPort ExternalInput {
        location: "socket://localhost:8082"     //It would be nice if this could be sent with as a parameter
        protocol: http{
            format = "json"
        }
        Interfaces: 
            SimpleConumerInterface
    }

    // Used for embedding services to talk with the inbox
    inputPort InboxInput {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: 
            SimpleConumerInterface,
            InboxInterface
    }

    // Used when forwarding messages back to embedder
    outputPort ConsumerInput {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: SimpleConumerInterface
    }
    embed Runtime as Runtime

    init {
        ConsumerInput.location << p.localLocation
        ExternalInput.location << p.externalLocation

        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "messageRetriever.ol"
            params << {
                localLocation << localLocation
            }
        })( MessageRetriever.location )
        println@Console( "Hey22" )(  )
    }

    main {
        [UpdateNumberForUser( req )( res ){
            println@Console("Hello from InboxService!")()
            connectionInfo << p.connectionInfo 
            scope ( MakeIdempotent ){
                    install( SQLException => {
                        println@Console("InboxService: \tTrying to insert message into inbox twice!")()
                        commitRequest.offset = consumeResponse.messages[i].offset
                        i++
                    })

            println@Console( "Inbox: Writing message for " + req.userToUpdate + " to table")()
            connect@Database( connectionInfo )(  )
            }


            response.code = 200
            response.reason = "Updated number locally"
        }]

        [RecieveKafka( req )( res ){
            with ( connectionInfo ) 
            {
                .username = "";
                .password = "";
                .host = "";
                .database = "file:database.sqlite"; // "." for memory-only
                .driver = "sqlite"
            }

            println@Console( "Inbox: Writing message for " + req + " to table")()
            connect@Database( connectionInfo )(  )

            res = "Nice"
        }]
    }
}