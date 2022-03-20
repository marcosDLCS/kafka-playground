package dev.mdlcs.kafka_bc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // Say hello!
        System.out.println("Hello from ProducerDemo!!!");

        // Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello word!");

        // Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(producerRecord);
        producer.close();
    }
}
