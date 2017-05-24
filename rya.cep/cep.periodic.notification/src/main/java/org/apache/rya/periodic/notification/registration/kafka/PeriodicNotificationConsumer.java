package org.apache.rya.periodic.notification.registration.kafka;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;

public class PeriodicNotificationConsumer implements Runnable {
    private KafkaConsumer<String, CommandNotification> consumer;
    private int m_threadNumber;
    private String topic;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private NotificationCoordinatorExecutor coord;
    private static final Logger LOG = Logger.getLogger(PeriodicNotificationConsumer.class);

    public PeriodicNotificationConsumer(String topic, KafkaConsumer<String, CommandNotification> consumer, int a_threadNumber,
            NotificationCoordinatorExecutor coord) {
        this.topic = topic;
        m_threadNumber = a_threadNumber;
        this.consumer = consumer;
        this.coord = coord;
    }

    public void run() {
        
        try {
            LOG.info("Creating kafka stream for consumer:" + m_threadNumber);
            consumer.subscribe(Arrays.asList(topic));
            while (!closed.get()) {
                ConsumerRecords<String, CommandNotification> records = consumer.poll(10000);
                // Handle new records
                for(ConsumerRecord<String, CommandNotification> record: records) {
                    CommandNotification notification = record.value();
                    LOG.info("Thread " + m_threadNumber + " is adding notification " + notification + " to queue.");
                    LOG.info("Message: " + notification);
                    coord.processNextCommandNotification(notification);
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }
    
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
