package com.mycompany.app;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import jolie.runtime.JavaService;

public class KafkaRelayer extends JavaService {

    private static String m_topicName = "local-demo";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = null;
        try {
            Properties props = PropertiesHelper.getProperties();
            producer = new KafkaProducer<String, String>(props);
            System.out.println("Hello World!");
            while (true) {
                ProducerRecord<String, String> message = new ProducerRecord<>(m_topicName, "Update",
                        String.format("Test %s", LocalDateTime.now().toString()));
                producer.send(message);
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            if (producer != null) {
                producer.close();
                System.out.println("Producer closed.");
            }
            e.printStackTrace();
        }
    }
}
