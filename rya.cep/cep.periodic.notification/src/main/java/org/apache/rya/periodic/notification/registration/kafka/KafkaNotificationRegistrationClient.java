package org.apache.rya.periodic.notification.registration.kafka;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.cep.periodic.api.Notification;
import org.apache.rya.cep.periodic.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;

public class KafkaNotificationRegistrationClient implements PeriodicNotificationClient {

    private KafkaProducer<String, CommandNotification> producer;
    private String topic;
    
    public KafkaNotificationRegistrationClient(String topic, KafkaProducer<String, CommandNotification> producer) {
        this.topic = topic;
        this.producer = producer;
    }
    
    @Override
    public void addNotification(PeriodicNotification notification) {
        processNotification(new CommandNotification(Command.ADD, notification));

    }

    @Override
    public void deleteNotification(BasicNotification notification) {
        processNotification(new CommandNotification(Command.DELETE, notification));
    }

    @Override
    public void deleteNotification(String notificationId) {
        processNotification(new CommandNotification(Command.DELETE, new BasicNotification(notificationId)));
    }

    @Override
    public void addNotification(String id, long period, long delay, TimeUnit unit) {
        Notification notification = PeriodicNotification.builder().id(id).period(period).initialDelay(delay).timeUnit(unit).build();
        processNotification(new CommandNotification(Command.ADD, notification));
    }
    
   
    private void processNotification(CommandNotification notification) {
        producer.send(new ProducerRecord<String, CommandNotification>(topic, notification.getId(), notification));
    }
    
    @Override
    public void close() {
        producer.close();
    }
    

}
