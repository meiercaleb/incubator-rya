package org.apache.rya.periodic.notification.registration.kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.cep.periodic.api.Notification;
import org.apache.rya.cep.periodic.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;

public class KafkaNotificationRegistrationClient implements PeriodicNotificationClient {

    private Properties props;
    private boolean init = false;
    private KafkaProducer<String, CommandNotification> producer;
    private String topic;
    
    public KafkaNotificationRegistrationClient(String topic, Properties props) {
        this.topic = topic;
        this.props = props;
    }
    
    
    @Override
    public void addNotification(PeriodicNotification notification) {
        validateState();
        processNotification(new CommandNotification(Command.ADD, notification));

    }

    @Override
    public void deleteNotification(BasicNotification notification) {
        validateState();
        processNotification(new CommandNotification(Command.DELETE, notification));
    }

    @Override
    public void deleteNotification(String notificationId) {
        validateState();
        processNotification(new CommandNotification(Command.DELETE, new BasicNotification(notificationId)));
    }

    @Override
    public void addNotification(String id, long period, long delay, TimeUnit unit) {
        validateState();
        Notification notification = PeriodicNotification.builder().id(id).period(period).initialDelay(delay).timeUnit(unit).build();
        processNotification(new CommandNotification(Command.ADD, notification));
    }
    
   
    private void processNotification(CommandNotification notification) {
        producer.send(new ProducerRecord<String, CommandNotification>(topic, notification.getId(), notification));
    }
    
    private void validateState() {
        if(!init) {
            init(props);
        }
    }
    
    private void init(Properties props) {
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());
        producer = new KafkaProducer<String, CommandNotification>(props);
        init = true;
    }


    @Override
    public void close() {
        producer.close();
    }
    

}
