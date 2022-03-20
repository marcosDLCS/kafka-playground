package dev.mdlcs.kafka_bc.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic            = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            // Assign and seek
            TopicPartition partition = new TopicPartition(topic, 0);
            var            offset    = 15L;
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);

            // Poll
            var maxMassages    = 5;
            var keepOnReading  = true;
            var currentMessage = 0;

            while (keepOnReading) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> rec : records) {
                    currentMessage++;

                    logger.info("Key: {}, Value: {}", rec.key(), rec.value());
                    logger.info("Partition: {}, Offset: {}", rec.partition(), rec.offset());

                    keepOnReading = currentMessage < maxMassages;
                    if(!keepOnReading) break;
                }
            }
        }
        logger.info("Exiting the application...");
    }
}
