package org.wikimedia.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        // custom class for reading in data from Kafka config file
        KafkaConfig kc = new KafkaConfig("src/main/resources/config.txt");

        //  define properties for connecting to Kafka cluster
        var props = new Properties();
        props.put("bootstrap.servers", kc.getBootstrapServer());
        props.put("sasl.mechanism", kc.getSaslMechanism());
        props.put("security.protocol", kc.getSecurityProtocol());
        props.put("sasl.jaas.config", kc.getSaslJaasConfig());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //  create producer, define topic and secondsToSleep
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "wikimedia.recentchange";
        int secondsToSleep = 5;

        // create event handler for reading recent changes stream from wikimedia
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start eventSource in different thread, producer sends messages through WikimediaChangeHandler
        eventSource.start();

        // produce for secondsToSleep seconds before terminating program
        TimeUnit.SECONDS.sleep(secondsToSleep);
    }
}
