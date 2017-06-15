package org.apache.rya.periodic.notification.registration.kafka;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.rya.pcj.fluo.test.base.KafkaExportITBase;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.junit.Assert;
import org.junit.Test;

public class PeriodicCommandNotificationConsumerIT extends KafkaExportITBase {

    private static final String  topic = "topic";
    
    @Test
    public void kafkaNotificationProviderTest() throws InterruptedException {
        
        BasicConfigurator.configure();
        
        BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        Properties props = createKafkaConfig();
        KafkaProducer<String, CommandNotification> producer = new KafkaProducer<>(props);
        KafkaNotificationRegistrationClient registration = new KafkaNotificationRegistrationClient(topic, producer);
        PeriodicNotificationCoordinatorExecutor coord = new PeriodicNotificationCoordinatorExecutor(2, notifications);
        KafkaNotificationProvider provider = new KafkaNotificationProvider(topic, new StringDeserializer(), new CommandNotificationSerializer(), props, coord, 1);
        provider.start();
        
        registration.addNotification("1", 1, 0, TimeUnit.SECONDS);
        registration.addNotification("2", 2, 0, TimeUnit.SECONDS);
        
        Thread.sleep(6200);
        registration.deleteNotification("1");
        registration.deleteNotification("2");
        
        //sleep for 4 seconds to ensure no more messages being produced
        Thread.sleep(4000);
        
        int count1 = 0;
        int count2 = 0;
        for(TimestampedNotification notification: notifications) {
            if(notification.getId().equals("1")) {
                count1++;
            }
            if(notification.getId().equals("2")) {
                count2++;
            }
        }
        
        Assert.assertEquals(6, count1);
        Assert.assertEquals(3, count2);
    }
    
    
    @Test
    public void kafkaNotificationMillisProviderTest() throws InterruptedException {
        
        BasicConfigurator.configure();
        
        BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        Properties props = createKafkaConfig();
        KafkaProducer<String, CommandNotification> producer = new KafkaProducer<>(props);
        KafkaNotificationRegistrationClient registration = new KafkaNotificationRegistrationClient(topic, producer);
        PeriodicNotificationCoordinatorExecutor coord = new PeriodicNotificationCoordinatorExecutor(2, notifications);
        KafkaNotificationProvider provider = new KafkaNotificationProvider(topic, new StringDeserializer(), new CommandNotificationSerializer(), props, coord, 1);
        provider.start();
        
        registration.addNotification("1", 1000, 0, TimeUnit.MILLISECONDS);
        registration.addNotification("2", 2000, 0, TimeUnit.MILLISECONDS);
        
        Thread.sleep(6200);
        registration.deleteNotification("1");
        registration.deleteNotification("2");
        
        //sleep for 4 seconds to ensure no more messages being produced
        Thread.sleep(4000);
        
        int count1 = 0;
        int count2 = 0;
        for(TimestampedNotification notification: notifications) {
            if(notification.getId().equals("1")) {
                count1++;
            }
            if(notification.getId().equals("2")) {
                count2++;
            }
        }
        
        Assert.assertEquals(6, count1);
        Assert.assertEquals(3, count2);
    }
    
    private Properties createKafkaConfig() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());

        return props;
    }
}