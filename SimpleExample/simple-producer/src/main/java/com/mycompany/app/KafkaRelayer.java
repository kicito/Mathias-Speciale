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

    private String m_topicName = "count-updates";

    public boolean updateCountForUsername(String username) {
        try {
            Properties props = PropertiesHelper.getProperties();
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
            ProducerRecord<String, String> message = new ProducerRecord<>(m_topicName, "UpdateUsername", username);
            producer.send(message);
            producer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
