include "inboxTypes.iol"

interface SimpleKafkaConsumerInterface {
    RequestResponse: 
        initialize( InitializeConsumerRequest ) ( KafkaOptions ),
        consume( ConsumeRequest )( ConsumerRecord ),
        commit( CommitRequest )( CommitResponse ) 
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