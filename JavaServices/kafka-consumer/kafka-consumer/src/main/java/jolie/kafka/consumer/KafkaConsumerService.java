package jolie.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

//  Kafka imports
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class KafkaConsumerService extends JavaService {
    private Object lock = new Object();
    private KafkaConsumer<String, String> consumer;
    private String topic;

    /**
     * @param input
     *              .bootstrapServer: string - The IP of the kafka server to use for
     *              bootstrapping onto the cluster, i.e. "localhost:9092"
     *              .groupId: string - This id of the group this consumer belongs
     *              to.
     *              .enableAutoCommit: 'true' | 'false' - Specifies whether kafka
     *              should automatically commit which messages this consumer has
     *              read
     *              .autoCommitInterval: string - The number of ms between auto
     *              commits, if enabled
     *              .topic: string - The topic this consumer should subscribe to
     * @return Same values as those given in input, as well as a status message
     *         '.reason'
     */
    public Value Initialize(Value input) {
        System.out.println("KafkaConsumerService: Initializing");
        Properties props = new Properties();
        Value kafkaOptions = input.getFirstChild("brokerOptions");
        Value pollOptions = input.getFirstChild("pollOptions");

        props.setProperty("bootstrap.servers", kafkaOptions.getFirstChild("bootstrapServer").strValue());
        props.setProperty("group.id", kafkaOptions.getFirstChild("groupId").strValue());
        props.setProperty("max.poll.records", pollOptions.getFirstChild("pollAmount").strValue());

        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        topic = kafkaOptions.getFirstChild("topic").strValue();

        synchronized (lock) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
        }

        Value response = Value.create();
        response.getFirstChild("topic").setValue(topic);
        response.getFirstChild("bootstrapServer").setValue(props.getProperty("bootstrap.servers"));
        response.getFirstChild("groupId").setValue(props.getProperty("group.id"));
        return response;
    }

    /**
     * @param input
     *              .timeoutMs: long - How long the consumer should wait for a
     *              response form Kafka
     * @return Value
     *         .code: int - Status code indicating success
     *         .messages: List<KafkaMessage> - The messages that kafka responded
     *         with
     */
    public Value Consume(Value input) {
        System.out.println("KafkaConsumerService: Polling for new messages in Kafka");
        long timeout = input.getFirstChild("timeoutMs").longValue();
        ConsumerRecords<String, String> records = null;

        Value response = Value.create();
        synchronized (lock) {
            if (consumer != null) {
                response.getFirstChild("status").setValue(200);
                records = consumer.poll(Duration.ofMillis(timeout));
            } else {
                System.out.println("Consumer not initialized!");
                response.getFirstChild("status").setValue(400);
            }
        }

        if (records != null) {
            System.out.println("KafkaConsumerService: Retrieved " + records.count() + " messages from kafka");
            for (ConsumerRecord<String, String> record : records) {
                Value message = Value.create();
                message.getFirstChild("offset").setValue(record.offset());
                message.getFirstChild("key").setValue(record.key());
                message.getFirstChild("value").setValue(record.value());
                message.getFirstChild("topic").setValue(record.topic());
                response.getChildren("messages").add(message);
            }
        }

        return response;
    }

    public Value Commit(Value input) {
        long offset = input.getFirstChild("offset").longValue() + 1; // +1 since we're committing what the offset of the
                                                                     // NEXT message is
        System.out.println("KafkaConsumerService: Comitting offset " + offset);
        Map<TopicPartition, OffsetAndMetadata> topics;
        synchronized (lock) {
            topics = consumer.partitionsFor(topic)
                    .stream()
                    .map(x -> new TopicPartition(x.topic(), x.partition()))
                    .collect(Collectors.toMap(x -> x,
                            x -> new OffsetAndMetadata(offset, topic)));
            consumer.commitSync(topics);
        }

        Value response = Value.create();
        response.getFirstChild("response").setValue("Committed offset " + offset + " for topic " + topic);
        return response;
    }
}
