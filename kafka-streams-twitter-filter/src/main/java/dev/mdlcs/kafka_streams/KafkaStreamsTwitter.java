package dev.mdlcs.kafka_streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsTwitter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Logger log = LoggerFactory.getLogger(KafkaStreamsTwitter.class.getName());

    public static void main(String[] args) throws InterruptedException {
        new KafkaStreamsTwitter().run();
    }

    public void run() throws InterruptedException {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kstreams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filtered = inputTopic.filter(
                (key, tweet) -> extractUserFollowersFromTweet(tweet) > 5000
        );
        filtered.to("important_tweets");

        // Build topology
        try (KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties)) {
            log.info("Starting...");
            kafkaStreams.start();

            Thread.sleep(50000);
        }

        log.info("Exiting...");
    }

    private Integer extractUserFollowersFromTweet(String jsonTweet) {
        try {
            return MAPPER.readTree(jsonTweet).get("user").get("followers_count").asInt();
        } catch (final Exception e) {
            log.error("ERROR: {}", jsonTweet);
            return 0;
        }
    }

}
