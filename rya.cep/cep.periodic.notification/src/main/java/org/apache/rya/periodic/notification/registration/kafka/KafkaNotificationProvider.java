package org.apache.rya.periodic.notification.registration.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.cep.periodic.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaNotificationProvider implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationProvider.class);
    private ConsumerConnector consumer;
    private String topic;
    private ExecutorService executor;
    private NotificationCoordinatorExecutor coord;
    private int numThreads;
    private boolean running = false;

    public KafkaNotificationProvider(String topic, Properties props, NotificationCoordinatorExecutor coord, int numThreads) {
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(props));
        this.coord = coord;
        this.numThreads = numThreads;
        this.topic = topic;
    }

    @Override
    public void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
        running = false;
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOG.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            LOG.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void start() {
        if(!coord.currentlyRunning()) {
            coord.start();
        }
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        LOG.info("Creating topic map with " + numThreads + " threads.");
        topicCountMap.put(topic, numThreads);
        Map<String, List<KafkaStream<String, CommandNotification>>> consumerMap = consumer.createMessageStreams(
                topicCountMap, new StringDecoder(new VerifiableProperties()), new CommandNotificationSerializer());
        List<KafkaStream<String, CommandNotification>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream<String, CommandNotification> stream : streams) {
            LOG.info("Creating consumer:" + threadNumber);
            executor.submit(new PeriodicNotificationConsumer(stream, threadNumber, coord));
            threadNumber++;
        }
        running = true;
    }

    private static ConsumerConfig createConsumerConfig(Properties props) {
        return new ConsumerConfig(props);
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
