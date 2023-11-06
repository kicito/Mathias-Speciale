
//#################### General types #######################\\
type KafkaOptions: void {   
    .bootstrapServer: string                   // The URL of the kafka server to connect to, e.g. "localhost:9092"
    .groupId: string                            // The group id of the kafka cluster to send messages to
    .topic: string
}

type RabbitMqOptions {      // Not implemented
    .bootstrapServer: string
    .groupId: string
}

type PollOptions: void{
    .pollDurationMS: int                        // How often the MessageForwarderService should check the database for new messages,
    .pollAmount: int                            // The amount of messages which are read from the 'messages' table per duration
}

type KafkaMessage {
    .offset: long
    .key: string
    .value: string
    .topic: string
}

//#################### InboxService types #######################
type InboxEmbeddingConfig: void {
    localLocation: any
    externalLocation[0,1]: string
    configFile[0, 1]: string
}

interface InboxInterface {
    RequestResponse: RecieveKafka( KafkaMessage )( string )
}

//#################### MessageRetrieverService types #######################
type ConsumeRequest{
    .timeoutMs: long
}

type ConsumerRecord {
    .status: int
    .messages*: KafkaMessage
}

type CommitRequest {
    .offset: long
}

type CommitResponse {
    .status: int
    .reason: string
}

type InitializeConsumerRequest {
    .pollOptions: PollOptions
    .brokerOptions: KafkaOptions   
}

interface MessageRetrieverInterface{

}
