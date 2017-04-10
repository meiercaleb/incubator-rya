package kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import api.Notification;
import api.PeriodicNotificationClient;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import notification.BasicNotification;
import notification.CommandNotification;
import notification.PeriodicNotification;
import notification.CommandNotification.Command;
import serialization.CommandNotificationSerializer;

public class KafkaNotificationRegistrationClient implements PeriodicNotificationClient {

    private Properties props;
    private boolean init = false;
    private Producer<String, CommandNotification> producer;
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
    public void addNotification(String id, long startTime, long period, long delay, TimeUnit unit) {
        validateState();
        Notification notification = PeriodicNotification.builder().id(id).period(period).initialDelay(delay).startTime(startTime).timeUnit(unit).build();
        processNotification(new CommandNotification(Command.ADD, notification));
    }
    
   
    private void processNotification(CommandNotification notification) {
        producer.send(new KeyedMessage<String, CommandNotification>(topic, notification.getId(), notification));
    }
    
    private void validateState() {
        if(!init) {
            init(props);
        }
    }
    
    private void init(Properties props) {
        props.setProperty("key.serializer.class", StringEncoder.class.getName());
        props.setProperty("serializer.class", CommandNotificationSerializer.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<String, CommandNotification>(producerConfig);
        init = true;
    }


    @Override
    public void close() {
        producer.close();
    }
    
    

}
