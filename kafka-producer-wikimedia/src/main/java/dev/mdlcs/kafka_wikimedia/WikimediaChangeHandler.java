package dev.mdlcs.kafka_wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final Logger                        log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());
    private final KafkaProducer<String, String> kafkaProducer;
    private final String                        topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic         = topic;
    }

    @Override
    public void onOpen() {
        // On open
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        log.info(messageEvent.getData());
        final var producerRecord = new ProducerRecord<String, String>(topic, messageEvent.getData());
        kafkaProducer.send(producerRecord);
    }

    @Override
    public void onComment(String s) throws Exception {
        // On comment
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error with stream... ", throwable);
    }
}
