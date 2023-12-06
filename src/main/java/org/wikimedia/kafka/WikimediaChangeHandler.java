package org.wikimedia.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
* implements EventHandler and adds a KafkaProducer object and topic to read from Wikimedia
* recent changes stream and send to a specified Kafka topic when receiving messages
* */
public class WikimediaChangeHandler implements EventHandler {
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kp, String topic){
        this.kafkaProducer = kp;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // unused
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent){
        // log event data
        log.info(messageEvent.getData());
        // send to Kafka topic
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        //unused
    }

    @Override
    public void onError(Throwable t) {
        log.error("Event stream read error", t);
    }
}
