include "console.iol"
include "time.iol"
include "database.iol"
include "file.iol"
include "Inbox/inboxTypes.iol"
include "serviceAInterface.iol"

from runtime import Runtime

service Inbox (p: InboxEmbeddingConfig){
    // Used for embedding services to talk with the inbox
    inputPort InboxInput {
        Location: "local"
        Interfaces: 
            InboxInterface
    }

    // Used when forwarding messages back to embedder
    outputPort EmbedderInput {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }
    embed Runtime as Runtime

    init
    {
        EmbedderInput.location = p.localLocation

        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "messageRetrieverService.ol"
            params << {
                localLocation << localLocation
            }
        })( MessageRetriever.location )

        readFile@File(
            {
                filename = "serviceBConfig.json"
                format = "json"
            }) ( config )

        scope ( createtable ) 
        {
            connect@Database( config.serviceAConnectionInfo )()
            update@Database( "CREATE TABLE IF NOT EXISTS inbox (request VARCHAR (150), hasBeenRead BOOLEAN, kafkaOffset INTEGER, UNIQUE(kafkaOffset));" )( ret )
        }
    }

    main{
        [RecieveKafka( req )( res ) {
            // Kafka messages for our inbox/outbox contains the operation invoked in the 'key', and the parameters in the 'value'
            connect@Database(config.serviceBConnectionInfo)()
            scope( MakeIdempotent ){
                // If this exception is thrown, Kafka some commit message must have disappeared. Resend it.
                install( SQLException => {
                    println@Console("Message already recieved, commit request")();
                    res = "Message already recieveid, please re-commit"
                })
                // Insert the request into the inbox table in the form:
                    // ___________________________________________________
                    // |            request        | hasBeenRead | offset |
                    // |———————————————————————————|—————————————|————————|
                    // | 'operation':'parameter(s)'|   'false'   | offset |
                    // |——————————————————————————————————————————————————|
                update@Database("INSERT INTO inbox VALUES (
                    \""+ req.key + ":" + req.value +        // numbersUpdated:user1
                    "\", false, " +                         // false
                    req.offset)()                           // offset
            }
            res << "Message stored"
        }] 
        {   
            // In the future, we might use Reflection to hit the correct method in the embedder.
            numbersUpdated@EmbedderInput()
        }
    }
}