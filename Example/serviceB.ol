include "database.iol"
include "console.iol"
include "time.iol"

from .outboxService import Outbox

type InboxUpdatedResponse {
    .amountMessagesRead: int
}

type InboxUpdatedRequest {
    .databaseConnectionInfo: ConnectionInfo
}

interface ChoreographyParticipantInterface {
    RequestResponse:
        inboxUpdated( InboxUpdatedRequest )( InboxUpdatedResponse )
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
        with ( connectionInfo ) 
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:serviceBDb.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }
        
        with ( pollSettings )
        {
            .pollAmount = 3
            .pollDurationMS = 3000
        }

        with ( kafkaOptions )
        {
            .topic =  "service-b-local-updates"
            .bootstrapServers =  "localhost:9092"
            .groupId = "test-group"
        }

        with ( outboxSettings )
        {
            .pollSettings << pollSettings;
            .databaseConnectionInfo << connectionInfo;
            .brokerOptions << kafkaOptions
        }
        
        connectKafka@OutboxService( outboxSettings ) ( outboxResponse )

        connect@Database( connectionInfo )( void )

        scope ( createtable ) 
        {
            install ( sqlexception => println@Console("numbers table already exists")() )
            updaterequest =
                "create table if not exists numbers(username varchar(50) not null, " +
                "number int)";
            update@Database( updaterequest )( ret )
        }

        checkInbox
    }

    define checkInbox{
        install( SQLException => println@Console("SQL Exception when checking inbox")() )
        // Read all messages in inbox
            // For each message m in inbox:
                // Call transactional outbox with queries that:
                    // Updates local state
                    // Deletes message m in inbox
                    // Inserts new message in outbox

        connect@Database( connectionInfo )( )       // This is referencing the connectionInfo that was set in 'init'
        query@Database( "SELECT * FROM inbox" )( inboxMessages )

        for ( row in inboxMessages.row ) {
            
            // Construct query which update local state:
            localUpdateQuery = "UPDATE Numbers SET number = number + 1 WHERE username = \"" + row.kafkaKey + "\""
            println@Console(localUpdateQuery)()

            // Construct query to delete message 'row' from inbox table
            inboxDeleteQuery = "DELETE FROM inbox WHERE kafkaOffset = " + row.kafkaOffset

            scope (ExecuteQueries){
                install ( SQLException => println@Console( "SQL exception while trying to insert data" )( ) )
                updateQuery.sqlQuery[0] = localUpdateQuery
                updateQuery.sqlQuery[1] = inboxDeleteQuery
            
                updateQuery.topic = "service-b-local-updates"
                updateQuery.key = row.kafkaKey
                updateQuery.value = "Updated number for " + row.kafkaKey
                transactionalOutboxUpdate@OutboxService( updateQuery )( updateResponse )
            }
        }
        
        println@Console("Service B has updated locally")()
        response.amountMessagesRead = #inboxMessages.row
    }

    main {
        [inboxUpdated( request )( response ){
            checkInbox
        }]
    }
}

