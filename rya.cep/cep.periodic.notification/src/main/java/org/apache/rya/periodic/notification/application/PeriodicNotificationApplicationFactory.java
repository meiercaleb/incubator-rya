package org.apache.rya.periodic.notification.application;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.fluo.api.client.FluoClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.cep.periodic.api.NotificationCoordinatorExecutor;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoClientFactory;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.exporter.BindingSetRecord;
import org.apache.rya.periodic.notification.exporter.KafkaExporterExecutor;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.processor.NotificationProcessorExecutor;
import org.apache.rya.periodic.notification.pruner.PeriodicQueryPrunerExecutor;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationProvider;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.openrdf.query.BindingSet;

public class PeriodicNotificationApplicationFactory {

    public static PeriodicNotificationApplication getPeriodicApplication(Properties props) throws PeriodicApplicationException {
        PeriodicNotificationApplicationConfiguration conf = new PeriodicNotificationApplicationConfiguration(props);
        Properties kafkaProps = getKafkaProperties(conf);

        BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        BlockingQueue<NodeBin> bins = new LinkedBlockingQueue<>();
        BlockingQueue<BindingSetRecord> bindingSets = new LinkedBlockingQueue<>();

        try {
            PeriodicQueryResultStorage storage = getPeriodicQueryResultStorage(conf);
            FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf);
            NotificationCoordinatorExecutor coordinator = getCoordinator(conf.getCoordinatorThreads(), notifications);
            KafkaExporterExecutor exporter = getExporter(conf.getExporterThreads(), kafkaProps, bindingSets);
            PeriodicQueryPrunerExecutor pruner = getPruner(storage, fluo, conf.getPrunerThreads(), bins);
            NotificationProcessorExecutor processor = getProcessor(storage, notifications, bins, bindingSets, conf.getProcessorThreads());
            KafkaNotificationProvider provider = getProvider(conf.getProducerThreads(), conf.getNotificationTopic(), coordinator, kafkaProps);
            return PeriodicNotificationApplication.builder().setCoordinator(coordinator).setProvider(provider).setExporter(exporter)
                    .setProcessor(processor).setPruner(pruner).build();
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new PeriodicApplicationException(e.getMessage());
        }
    }

    private static NotificationCoordinatorExecutor getCoordinator(int numThreads, BlockingQueue<TimestampedNotification> notifications) {
        return new PeriodicNotificationCoordinatorExecutor(numThreads, notifications);
    }

    private static KafkaExporterExecutor getExporter(int numThreads, Properties props, BlockingQueue<BindingSetRecord> bindingSets) {
        KafkaProducer<String, BindingSet> producer = new KafkaProducer<>(props, new StringSerializer(), new BindingSetSerDe());
        return new KafkaExporterExecutor(producer, numThreads, bindingSets);
    }

    private static PeriodicQueryPrunerExecutor getPruner(PeriodicQueryResultStorage storage, FluoClient fluo, int numThreads,
            BlockingQueue<NodeBin> bins) {
        return new PeriodicQueryPrunerExecutor(storage, fluo, numThreads, bins);
    }

    private static NotificationProcessorExecutor getProcessor(PeriodicQueryResultStorage periodicStorage,
            BlockingQueue<TimestampedNotification> notifications, BlockingQueue<NodeBin> bins, BlockingQueue<BindingSetRecord> bindingSets,
            int numThreads) {
        return new NotificationProcessorExecutor(periodicStorage, notifications, bins, bindingSets, numThreads);
    }

    private static KafkaNotificationProvider getProvider(int numThreads, String topic, NotificationCoordinatorExecutor coord,
            Properties props) {
        return new KafkaNotificationProvider(topic, new StringDeserializer(), new CommandNotificationSerializer(), props, coord,
                numThreads);
    }

    private static PeriodicQueryResultStorage getPeriodicQueryResultStorage(PeriodicNotificationApplicationConfiguration conf)
            throws AccumuloException, AccumuloSecurityException {
        Instance instance = new ZooKeeperInstance(conf.getAccumuloInstance(), conf.getAccumuloZookeepers());
        Connector conn = instance.getConnector(conf.getAccumuloUser(), new PasswordToken(conf.getAccumuloPassword()));
        String ryaInstance = conf.getTablePrefix();
        return new AccumuloPeriodicQueryResultStorage(conn, ryaInstance);
    }
    
    private static Properties getKafkaProperties(PeriodicNotificationApplicationConfiguration conf) { 
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootStrapServers());
        kafkaProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, conf.getNotificationClientId());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, conf.getNotificationGroupId());
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaProps;
    }

}
