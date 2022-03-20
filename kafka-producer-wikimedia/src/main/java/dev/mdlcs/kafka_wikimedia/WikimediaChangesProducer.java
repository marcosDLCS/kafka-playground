package dev.mdlcs.kafka_wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC             = "wikimedia.recentchange";
    private static final String STREAM_URL        = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        new WikimediaChangesProducer().run();
    }

    public void run() throws InterruptedException {

        // Basic Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe producer
        // (enabled by default in Kafka > 3.0)

        // acks = -1
        // enable.idempotence = true
        // max.in.flight.requests.per.connection = 5
        // delivery.timeout.ms = 120000
        // retries = 2147483647

        // High throughput producer
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Event Source
        EventHandler        eventHandler = new WikimediaChangeHandler(producer, TOPIC);
        EventSource.Builder builder      = new EventSource.Builder(eventHandler, URI.create(STREAM_URL));
        EventSource         eventSource  = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(5);
    }
}
