package org.wikimedia.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testProducer {
        private static final Logger log = LoggerFactory.getLogger(testProducer.class.getSimpleName());

        public static void main(String[] args) {
            //  connect to upstash server
            var props = new Properties();
            KafkaConfig kc = new KafkaConfig("src/main/resources/config.txt");

            props.put("bootstrap.servers", kc.getBootstrapServer());
            props.put("sasl.mechanism", kc.getSaslMechanism());
            props.put("security.protocol", kc.getSecurityProtocol());
            props.put("sasl.jaas.config", kc.getSaslJassConfig());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            //  create producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            for(int i = 0; i < 10; i++){
                //  create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("second_topic", "Producer: " + i);

                // send producer record with callback
                producer.send(producerRecord, new Callback() {
                    //executes when record is sent successfully, or throws exception
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Received metadata:" +
                                    "\nTopic: " + metadata.topic() +
                                    "\nPartition: " + metadata.partition() +
                                    "\nOffset: " + metadata.offset() +
                                    "\nTimestamp: " + metadata.timestamp()
                            );
                        } else {
                            log.error("Producer error", e);
                        }
                    }
                });
            }

            // flush and close producer, producer.flush() to flush without closing
            producer.close();
        }
}
