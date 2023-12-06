package org.wikimedia.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
/*
* Simple consumer class for WikimediaChangesProducer with graceful shutdown. Reads and logs messages
* sent by WikimediaChangesProducer
* */
public class WikimediaChangesConsumer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesConsumer.class.getSimpleName());

    public static void main(String[] args) {
        String groupID = "wikimedia-consumer";
        String topic = "wikimedia.recentchange";

        // custom class for reading in data from Kafka config file
        KafkaConfig kc = new KafkaConfig("src/main/resources/config.txt");

        //  connect to upstash server
        var props = new Properties();
        props.put("bootstrap.servers", kc.getBootstrapServer());
        props.put("sasl.mechanism", kc.getSaslMechanism());
        props.put("security.protocol", kc.getSecurityProtocol());
        props.put("sasl.jaas.config", kc.getSaslJaasConfig());

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupID);
        props.put("auto.offset.reset", "earliest");

        //  create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // get reference to main thread
        final Thread mainThread = Thread.currentThread();

        //add shutdown hook, will throw exception within while loop
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown detected, exiting with consumer.wakeup()");
                consumer.wakeup();
                //join main thread to allow execution of its code
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                }
            }
        } catch(WakeupException we) {
            log.info("Consumer shutting down");
        } catch(Exception e){
            log.error("Unexpected consumer error: ", e);
        } finally {
            //close consumer and commit offsets
            consumer.close();
        }
    }
}
