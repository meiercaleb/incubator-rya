package kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import api.NotificationCoordinatorExecutor;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import notification.CommandNotification;

public class PeriodicNotificationConsumer implements Runnable {
    private KafkaStream<String, CommandNotification> m_stream;
    private int m_threadNumber;
    private NotificationCoordinatorExecutor coord;
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicNotificationConsumer.class);

    public PeriodicNotificationConsumer(KafkaStream<String, CommandNotification> a_stream, int a_threadNumber,
            NotificationCoordinatorExecutor coord) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.coord = coord;
    }

    public void run() {
        ConsumerIterator<String, CommandNotification> it = m_stream.iterator();
        LOG.info("Creating kafka stream for consumer:" + m_threadNumber);
        MessageAndMetadata<String, CommandNotification> messageAndMeta = null;
        while (it.hasNext()) {
            messageAndMeta = it.next();
            LOG.info("Thread " + m_threadNumber + " is adding notification " + messageAndMeta.message() + " to queue.");
            coord.processNextCommandNotification(messageAndMeta.message());
        }
        LOG.info("Shutting down Thread: " + m_threadNumber);
    }
}
