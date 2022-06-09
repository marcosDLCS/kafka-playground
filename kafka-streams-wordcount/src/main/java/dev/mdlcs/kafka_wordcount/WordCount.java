package dev.mdlcs.kafka_wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public static void main(String[] args) {
        new WordCount().run();
    }

    public void run() {
        System.out.println("Starting word count...");

        final var properties = createConfig();

        final StreamsBuilder          builder = new StreamsBuilder();
        final KStream<String, String> input   = builder.stream("word-count-in");

        final KTable<String, Long> wordCounts = input
                .mapValues(line -> line.toLowerCase())
                .flatMapValues(lcLine -> Arrays.asList(lcLine.split("\\W+")))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Materialized.as("counts"));

        wordCounts.toStream().to("word-count-out", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Print the topology
        while (true) {
            streams.localThreadsMetadata().forEach(System.out::println);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private Properties createConfig() {
        final Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return properties;
    }
}
