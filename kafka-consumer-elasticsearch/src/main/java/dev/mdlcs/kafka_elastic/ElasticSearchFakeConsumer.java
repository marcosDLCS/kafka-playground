package dev.mdlcs.kafka_elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class ElasticSearchFakeConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Logger log = LoggerFactory.getLogger(ElasticSearchFakeConsumer.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        new ElasticSearchFakeConsumer().run();
    }

    public void run() throws IOException, InterruptedException {

        var consumer = createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            log.info("Received {} records...", records.count());
            for (ConsumerRecord<String, String> rec : records) {
                var id = extractIdFromTweet(rec.value());
                log.info("Id: {} - Value: {}", id, rec.value());
                Thread.sleep(25);
            }

            log.info("Committing offsets...");
            consumer.commitSync();
            log.info("Successfully committed offsets...");
            Thread.sleep(250);
        }
    }

    private KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId          = "kafka_twitter";
        String autoOffsetReset  = "earliest";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        // Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private String extractIdFromTweet(String jsonTweet) throws JsonProcessingException {
        try {
            return MAPPER.readTree(jsonTweet).get("id_str").asText();
        } catch (final Exception e) {
            log.error("Ex: ", e);
            return UUID.randomUUID().toString();
        }
    }

}
