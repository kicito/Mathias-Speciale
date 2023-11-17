include "Outbox/outboxTypes.iol"

from runtime import Runtime
from .messageForwarderService import MessageForwarderInterface
from ..TransactionService.transactionService import TransactionServiceInterface

interface OutboxInterface{
    RequestResponse:
        updateOutbox( UpdateOutboxRequest )( StatusResponse )
}

/**
* This service is used to implement the outbox pattern. Given an SQL query and some message, it will atomically execute the query, as well write the message to a messages table.
* It will then embeds a 'RelayService', which reads from the 'Messages' table and forwards messages into Kafka.
*/
service Outbox(p: OutboxSettings){
    execution: sequential
    inputPort OutboxPort {
        Location: "local"
        Interfaces: OutboxInterface
    }

    outputPort TransactionService {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: TransactionServiceInterface
    }

    outputPort MFS {
        Location: "local"   // Overwritten in init
        Protocol: http{
            format = "json"
        }
        Interfaces: MessageForwarderInterface
    }
    embed Runtime as Runtime

    init {
        // Insert location of the transaction service embedded in main service
        TransactionService.location << p.transactionServiceLocation

        // Load MFS
        loadEmbeddedService@Runtime({
            filepath = "messageForwarderService.ol"
        })( MFS.location )

        println@Console("OutboxService: \tInitializing connection to Kafka")();
        connect@TrnasactionService( p.databaseConnectionInfo )( void )
        connect@Database( p.databaseConnectionInfo )( void )
        scope ( createMessagesTable )
        {
            install ( SQLException => { println@Console("Error when creating the outbox table for the outbox!")() })

            // Varchar size is not enforced by sqlite, we can insert a string of any length
            updateRequest = "CREATE TABLE IF NOT EXISTS outbox (kafkaKey VARCHAR(50), kafkaValue VARCHAR (150), mid SERIAL PRIMARY KEY);"
            update@Database( updateRequest )( ret )
        }

        relayRequest.databaseConnectionInfo << p.databaseConnectionInfo
        relayRequest.pollSettings << p.pollSettings

        relayRequest.columnSettings.keyColumn = "kafkaKey"
        relayRequest.columnSettings.valueColumn = "kafkaValue"
        relayRequest.columnSettings.idColumn = "mid"
        relayRequest.brokerOptions << p.brokerOptions
        
        startReadingMessages@MFS( relayRequest )

    }
    
    main {
        [updateOutbox( req )( res ){
            if (global.M_MessageBroker == "Kafka"){
                install (ConnectionError => response = "Call to update before connecting" )

                updateOutboxTable = "INSERT INTO outbox (kafkaKey, kafkaValue) VALUES (\"" + req.key + "\", \"" + req.value + "\");" 

                with ( updateRequest ){
                    .handle = req.tHandle;
                    .update = updateOutboxTable
                }

                executeUpdate@TransactionService( updateRequest )( updateResponse )
                if ( req.commitTransaction ){
                    commit@TransactionSErvice( req.tHandle )()
                }
                res.success = true 
                res.message = "Transaction executed sucessfully"
            }
        }]
    }
}