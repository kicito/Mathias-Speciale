include "database.iol"
include "console.iol"
include "time.iol"
include "file.iol"
include "runtime.iol"

from .outboxService import Outbox

type InboxUpdatedResponse {
    .amountMessagesRead: int
}

interface ChoreographyParticipantInterface {
    RequestResponse:
        inboxUpdated( string )( InboxUpdatedResponse )
}

service ServiceB{
    execution{ sequential }

    // This port allows for calling the consumer locally
    inputPort ServiceBSocket {
        location: "socket://localhost:8082" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyParticipantInterface
    }

    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ChoreographyParticipantInterface
    }
    embed Outbox as OutboxService


    init {
        readFile@File(
            {
                filename = "serviceBConfig.json"
                format = "json"
            }) ( config )

        with ( outboxSettings )
        {
            .pollSettings << config.pollOptions;
            .databaseConnectionInfo << config.serviceBConnectionInfo;
            .brokerOptions << config.kafkaOutboxOptions
        }

        connectKafka@OutboxService( outboxSettings )
        connect@Database( config.serviceBConnectionInfo )( void )

        scope ( createtable ) 
        {
            updaterequest = "CREATE TABLE IF NOT EXISTS numbers(username VARCHAR(50) NOT null, number INT)"
            update@Database( updaterequest )( ret )
        }
        checkInbox
    }

    // This procedure requires an existing connection to a DB
    define checkInbox{
        scope (one){
            install( SQLException => checkInbox )
            query@Database( "SELECT * FROM inbox" )( inboxMessages )
        }

        for ( row in inboxMessages.row ) {
            println@Console("ServiceB: Checking inbox found update request for " + row.kafkaKey)()
            query@Database("SELECT * FROM Numbers WHERE username = \"" + row.kafkaKey + "\"")( userExists )
            // Construct query which updates local state:
            if (#userExists.row < 1){
                    localUpdateQuery = "INSERT INTO Numbers VALUES (\"" + row.kafkaKey + "\", 0);"
                } else{
                    localUpdateQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + row.kafkaKey + "\""
                }

            // Construct query to delete message row from inbox table
            inboxDeleteQuery = "DELETE FROM inbox WHERE kafkaOffset = " + row.kafkaOffset

            scope (ExecuteQueries){
                updateQuery.sqlQuery[0] = localUpdateQuery
                updateQuery.sqlQuery[1] = inboxDeleteQuery
            
                updateQuery.topic = "service-b-local-updates"
                updateQuery.key = row.kafkaKey
                updateQuery.value = "Updated number for " + row.kafkaKey
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
                println@Console("Service B has updated locally")()
            }
        }
        response.amountMessagesRead = #inboxMessages.row
    }

    main {
        [inboxUpdated( request )( response ){
            connect@Database(config.serviceBConnectionInfo)()
            checkInbox
        }]
    }
}

