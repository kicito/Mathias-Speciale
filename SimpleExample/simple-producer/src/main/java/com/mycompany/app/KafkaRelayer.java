package com.mycompany.app;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import jolie.runtime.JavaService;

public class KafkaRelayer extends JavaService {

    private static String m_topicName = "local-demo";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer;
        try {
            Properties props = PropertiesHelper.getProperties();
            producer = new KafkaProducer<>(props);
            ProducerRecord<String, String> message = new ProducerRecord<>(m_topicName, "Test", "3");
            producer.send(message);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Hello World!");
    }
}
