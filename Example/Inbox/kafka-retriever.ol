include "inboxTypes.iol"

interface SimpleKafkaConsumerInterface {
    RequestResponse: 
        Initialize( InitializeConsumerRequest ) ( KafkaOptions ),
        Consume( ConsumeRequest )( ConsumerRecord ),
        Commit( CommitRequest )( CommitResponse ) 
}

service KafkaConsumer{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConsumerInterface
        } 
        foreign java {
            class: "jolie.kafka.consumer.KafkaConsumerService"
        }
}