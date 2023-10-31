include "console.iol"
include "time.iol"
include "inboxTypes.iol"
include "simpleConsumerInterface.iol"
from .IOBoxController import IOBoxControllerInterface

from database import Database
from runtime import Runtime
service Inbox(p: InboxEmbeddingConfig){
    execution: concurrent

    // This service takes over handling of the external endpoint from the embedder
    inputPort ExternalInput {
        location: "socket://localhost:8082"     //It would be nice if this could be sent with as a parameter
        protocol: http{
            format = "json"
        }
        Interfaces: SimpleConsumerInterface  
    }

    // Used for embedding services to talk with the inbox
    inputPort InboxInput {
        location: "local"
        protocol: http{
            format = "json"
        }
        interfaces: 
            SimpleConsumerInterface,
            InboxInterface
    }

    // Used when forwarding messages back to embedder
    outputPort InboxController {
        location: "local"   // Overwritten in init
        protocol: http{
            format = "json"
        }
        interfaces: IOBoxControllerInterface
    }
    embed Runtime as Runtime
    embed Database as Database

    init {
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:database.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        // Set the location for communication with the embedder
        InboxController.location << p.localLocation
        ExternalInput.location << p.externalLocation

        //Load the service which is responsible for retriving Kafka messages
        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "messageRetriever.ol"
            params << {
                localLocation << localLocation
            }
        })( lol )

        connect@Database(connectionInfo)()
        update@Database("CREATE TABLE IF NOT EXISTS inbox (request VARCHAR (150), hasBeenRead BOOLEAN, kafkaOffset INTEGER, rowid INTEGER PRIMARY KEY AUTOINCREMENT, UNIQUE(kafkaOffset));")()
    }

    main {
        [updateNumberForUser( req )( res ){
            update@Database("INSERT INTO inbox (request, hasBeenRead, kafkaOffset) VALUES (\"udateNumber:" + req.userToUpdate + "\", false, NULL)")()
            res = "Message received"
        }] 
        {
            inboxTableUpdated@InboxController( req.userToUpdate )
        }

        [recieveKafka( req )( res ){
            install( SQLException => {
                    println@Console("Message already recieved, commit request")();
                    res = "Message already recieveid, please re-commit"
            })
            
            update@Database("INSERT INTO inbox (request, hasBeenRead, kafkaOffset) VALUES (
                \""+ req.key + ":" + req.value +        
                "\", false, " +
                req.offset + ")")()
            res = "Message received"
        }]
        {
            inboxTableUpdated@InboxController( req.username )
        }
    }
}