package dev.mdlcs.kafka_twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static final String CONSUMER_KEY    = "<CONSUMER_KEY>";
    private static final String CONSUMER_SECRET = "<CONSUMER_SECRET>";
    private static final String TOKEN           = "<TOKEN>";
    private static final String TOKEN_SECRET    = "<TOKEN_SECRET>";

    private static final List<String> TERMS = List.of("Java", "Kotlin", "Kafka");

    public TwitterProducer() {
        // TwitterProducer
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        log.info("Starting application...");

        // Twitter client
        log.info("Configuring client...");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        var                   client   = createTwitterClient(msgQueue);
        client.connect();

        // Kafka producer
        var producer = createKafkaProducer();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down the application...");
            log.info("Stopping Twitter client...");
            client.stop();
            log.info("Closing Kafka producer...");
            producer.close();
            log.info("Bye!");
        }));

        // Loop
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                client.stop();
            }
            if (msg != null) {
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        log.error("Something bad happened...");
                    }
                });
            }
        }
        log.info("End of application...");

    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {


        Hosts                  hosebirdHosts    = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(TERMS);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "35");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(64 * 1024));

        // Producer
        return new KafkaProducer<>(properties);
    }
}
