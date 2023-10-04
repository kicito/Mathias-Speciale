include "database.iol"
include "console.iol"
include "time.iol"

type KafkaOptions {

}

type RabbitMqOptions {

}

type PollSettings{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type UpdateOutboxRequest{
    .sqlQuery: string                                   // The query that is to be executed against the database
    .key : string                                       // The key to use in the kafka message
    .value : string                                     // The value for the kafka message
    .topic : string                                     // The kafka topic on which the update should be broadcast
}

type UpdateOutboxResponse: string

type ConnectOutboxRequest{
    .databaseConnectionInfo: ConnectionInfo             // The connectioninfo used to connect to the database. See docs the Database module.
    .pollSettings: PollSettings                         // Object of type PollSettings descibing the desired behaviour of the MessageForwarder
    .messageBroker: string(enum(["kafka", "rabbitmq"])) // Which message broker is being used
    .brokerOptions: KafkaOptions | RabbitMqOptions
}

interface OutboxInterface{
    OneWay:
        connect( ConnectOutboxRequest )
    RequestResponse:
        transactionalOutboxUpdate( UpdateOutboxRequest )( UpdateOutboxResponse )
}