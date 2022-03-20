package dev.mdlcs.kafka_bc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithCallbackAndKeys {

    public static void main(String[] args) {

        final String METADATA = """
                                
                Received new metadata.
                Topic    : {}
                Partition: {}
                Offset   : {}
                Timestamp: {}
                """;

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackAndKeys.class);

        // Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 21).forEach(number -> {
            // Topic
            var topic = "first_topic";
            // Key
            var key = "id_" + number;
            logger.info("Key: {}", key);
            // Value
            var value = "Hello word! (with callback) -> " + number;

            // Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info(
                            METADATA,
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            });
        });
        producer.close();
    }
}
