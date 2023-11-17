type StatusResponse {
    .success: bool
    .message: string
}
type KafkaMessage {
    .topic: string
    .key: string
    .value: string
    .brokerOptions: KafkaOptions
}

type KafkaOptions: void {   
    .topic: string                              // The topic to write updates to
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
}

interface KafkaInserterInterface {
    RequestResponse: propagateMessage ( KafkaMessage )( StatusResponse )
}

/**
*   A connector class to allow for communicating with the java-service (defined in ~/JavaServices/kafka-producer/src/main/java/example/KafkaRelayer.java)
*/
service KafkaInserter{
    inputPort Input {
        Location: "local"
        Interfaces: KafkaInserterInterface
        }
        foreign java {
            class: "example.KafkaRelayer"
        }
}