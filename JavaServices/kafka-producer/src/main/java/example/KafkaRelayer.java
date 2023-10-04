package example;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
//  Kafka imports
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaRelayer extends JavaService {

    public void propagateMessage(Value input) {

        System.out.println(input.toString());
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("max.poll.records", "10");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "500");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> message = new ProducerRecord<>(
                input.getFirstChild("topic").strValue(),
                input.getFirstChild("key").strValue(),
                input.getFirstChild("value").strValue());

        Callback cb = new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                String response;
                if (e == null) {
                    response = "Kafka message delivered. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp();
                    System.out.println(response);

                } else {
                    response = e.getMessage();
                    System.out.println(response);
                }
            }
        };
        producer.send(message, cb);
        producer.close();
    }
}
