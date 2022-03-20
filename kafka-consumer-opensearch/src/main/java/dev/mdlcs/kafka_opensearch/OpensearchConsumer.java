package dev.mdlcs.kafka_opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.opensearch.client.RequestOptions.DEFAULT;

public class OpensearchConsumer {

    // https://github.com/conduktor/kafka-beginners-course/blob/main/kafka-consumer-opensearch

    private static final Logger LOG                 = LoggerFactory.getLogger(OpensearchConsumer.class.getSimpleName());
    private static final String OPENSEARCH_LOCATION = "http://localhost:9200";
    private static final String WIKIMEDIA_INDEX     = "wikimedia";

    public static void main(String[] args) throws IOException {
        new OpensearchConsumer().run();
    }

    public void run() throws IOException {

        // Create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        try (openSearchClient; kafkaConsumer) {
            if (!openSearchClient.indices().exists(new GetIndexRequest(WIKIMEDIA_INDEX), DEFAULT)) {

                CreateIndexRequest createIndexRequest = new CreateIndexRequest(WIKIMEDIA_INDEX);
                openSearchClient.indices().create(createIndexRequest, DEFAULT);

                LOG.info("The 'wikimedia' OpenSearch index has been created...");
            } else {
                LOG.info("The 'wikimedia' OpenSearch index is already created...");
            }

            kafkaConsumer.subscribe(List.of("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                LOG.info("Received {} records...", records.count());

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        IndexRequest idxReq = new IndexRequest(WIKIMEDIA_INDEX)
                                .source(rec.value(), XContentType.JSON)
                                .id(createId(rec));
                        bulkRequest.add(idxReq);
                    } catch (final Exception e) {
                        LOG.error("Record with exception: {}", rec.value());
                    }
                }

                // Bulk request
                if (bulkRequest.numberOfActions() > 0) {

                    // Perform request
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, DEFAULT);
                    LOG.info("Inserted {} record(s) in OpenSearch...", bulkResponse.getItems().length);

                    // Wait
                    try {
                        TimeUnit.MILLISECONDS.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // Commit offsets
                    kafkaConsumer.commitSync();
                    LOG.info("The offsets have been committed...");
                }
            }
        }
    }

    private static String createId(ConsumerRecord<String, String> rec) {
        final String kafkaCoordsId = rec.topic() + "_" + rec.partition() + "_" + rec.offset();
        try {
            return JsonParser.parseString(rec.value())
                    .getAsJsonObject()
                    .get("meta")
                    .getAsJsonObject()
                    .get("id")
                    .getAsString();
        } catch (final Exception e) {
            LOG.error("Error retrieving ID from message. Assigning kafka coordinates ID...");
            return kafkaCoordsId;
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId          = "consumer-opensearch";
        String autoOffsetReset  = "latest";
        String autoCommit       = "false";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        //
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);

        // Consumer
        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient() {

        RestHighLevelClient restHighLevelClient;
        URI                 connUri  = URI.create(OPENSEARCH_LOCATION);
        String              userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(),
                    connUri.getPort(), "http")));

        } else {
            String[]            auth = userInfo.split(":");
            CredentialsProvider cp   = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }
}
