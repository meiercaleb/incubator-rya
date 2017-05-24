package org.apache.rya.periodic.notification.registration.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.cep.periodic.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaNotificationProvider implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationProvider.class);
    private String topic;
    private ExecutorService executor;
    private NotificationCoordinatorExecutor coord;
    private Properties props;
    private int numThreads;
    private boolean running = false;
    List<PeriodicNotificationConsumer> consumers;

    public KafkaNotificationProvider(String topic, Properties props, NotificationCoordinatorExecutor coord, int numThreads) {
        this.coord = coord;
        this.numThreads = numThreads;
        this.topic = topic;
        this.props = props;
        this.consumers = new ArrayList<>();
    }

    @Override
    public void stop() {
        if (consumers != null && consumers.size() > 0) {
            for(PeriodicNotificationConsumer consumer: consumers) {
                consumer.shutdown();
            }
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
        // now launch all the threads
        executor = Executors.newFixedThreadPool(numThreads);

        // now create consumers to consume the messages
        int threadNumber = 0;
        for (int i = 0; i < numThreads; i++) {
            LOG.info("Creating consumer:" + threadNumber);
            KafkaConsumer<String, CommandNotification> consumer = new KafkaConsumer<>(props);
            PeriodicNotificationConsumer periodicConsumer = new PeriodicNotificationConsumer(topic, consumer, threadNumber, coord);
            consumers.add(periodicConsumer);
            executor.submit(periodicConsumer);
            threadNumber++;
        }
        running = true;
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
