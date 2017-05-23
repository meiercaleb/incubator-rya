package org.apache.rya.periodic.notification.application;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_INSTANCE;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_PASSWORD;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_USERNAME;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_ZOOKEEPERS;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.FLUO_APP_NAME;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.exporter.KafkaExporterExecutor;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.processor.NotificationProcessorExecutor;
import org.apache.rya.periodic.notification.pruner.PeriodicQueryPrunerExecutor;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationProvider;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

public class PeriodicNotificationApplication implements LifeCycle {

    private static final Logger log = Logger.getLogger(PeriodicNotificationApplication.class);
    private Properties props;
    private PeriodicNotificationCoordinatorExecutor coordinator;
    private KafkaNotificationProvider provider;
    private PeriodicQueryPrunerExecutor pruner;
    private NotificationProcessorExecutor processor;
    private KafkaExporterExecutor exporter;
    private BlockingQueue<TimestampedNotification> notifications;
    private BlockingQueue<BindingSet> bindingSets;
    private BlockingQueue<NodeBin> bins;
    private boolean running = false;
    
    

    public PeriodicNotificationApplication(Properties props) {
        Preconditions.checkNotNull(props);
        this.props = props;
        notifications = new LinkedBlockingQueue<>();
        bins = new LinkedBlockingQueue<>();
        bindingSets = new LinkedBlockingQueue<>();
    }

    @Override
    public void start() {
        AccumuloRdfConfiguration conf = getConfigFromProps(props);
        FluoClient client = getFluoClientFromConfig(conf);
        checkArgument(props.contains("bootstrap.servers"), "Missing configuration: bootstrap.servers");
        coordinator = new PeriodicNotificationCoordinatorExecutor(getThreadNumber("coordinator.threads"), notifications);
        provider = new KafkaNotificationProvider(props.getProperty("kafka.producer.topic"), props, coordinator, getThreadNumber("producer.threads"));
        processor = new NotificationProcessorExecutor(client, conf, notifications, bins, bindingSets,
                getThreadNumber("processor.threads"));
        pruner = new PeriodicQueryPrunerExecutor(conf, client, getThreadNumber("pruner.threads"), bins);
        exporter = new KafkaExporterExecutor(props.getProperty("kafka.exporter.topic"), props, getThreadNumber("exporter.threads"), bindingSets);
        coordinator.start();
        provider.start();
        processor.start();
        pruner.start();
        exporter.start();
        running = true;
    }
    
    private int getThreadNumber(String key) {
        props.contains(key);
        return Integer.valueOf(props.getProperty(key));
    }

    @Override
    public void stop() {
        provider.stop();
        coordinator.stop();
        processor.stop();
        pruner.stop();
        exporter.stop();
        running = false;
    }

    private AccumuloRdfConfiguration getConfigFromProps(Properties props) {
        
        try {
            AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
            conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, props.getProperty("accumulo.rya.prefix"));
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, props.getProperty("accumulo.auths"));
            conf.set(ConfigUtils.CLOUDBASE_USER, props.getProperty("accumulo.user"));
            conf.set(ConfigUtils.CLOUDBASE_PASSWORD, props.getProperty("accumulo.password"));
            conf.set(ConfigUtils.CLOUDBASE_INSTANCE, props.getProperty("accumulo.instance"));
            conf.set(ConfigUtils.FLUO_APP_NAME, props.getProperty("fluo.app.name"));
            conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, props.getProperty("accumulo.zookeepers"));
            return conf;
        } catch (Exception e) {
            log.warn(
                    "The following properties must be non-null: accumulo.rya.prefix, accumulo.user, accumulo.auths, accumulo.password, accumulo.instance, fluo.app.name, accumulo.zookeepers");
            throw new RuntimeException(e);
        }
    }

    private FluoClient getFluoClientFromConfig(Configuration conf) {

        // Ensure the correct updater type has been set.
        FluoPcjUpdaterConfig fluoUpdaterConfig = new FluoPcjUpdaterConfig(conf);

        // Make sure the required values are present.
        checkArgument(fluoUpdaterConfig.getFluoAppName().isPresent(), "Missing configuration: " + FLUO_APP_NAME);
        checkArgument(fluoUpdaterConfig.getFluoZookeepers().isPresent(), "Missing configuration: " + ACCUMULO_ZOOKEEPERS);
        checkArgument(fluoUpdaterConfig.getAccumuloZookeepers().isPresent(), "Missing configuration: " + ACCUMULO_ZOOKEEPERS);
        checkArgument(fluoUpdaterConfig.getAccumuloInstance().isPresent(), "Missing configuration: " + ACCUMULO_INSTANCE);
        checkArgument(fluoUpdaterConfig.getAccumuloUsername().isPresent(), "Missing configuration: " + ACCUMULO_USERNAME);
        checkArgument(fluoUpdaterConfig.getAccumuloPassword().isPresent(), "Missing configuration: " + ACCUMULO_PASSWORD);

        // Fluo configuration values.
        final FluoConfiguration fluoClientConfig = new FluoConfiguration();
        fluoClientConfig.setApplicationName(fluoUpdaterConfig.getFluoAppName().get());
        fluoClientConfig.setInstanceZookeepers(fluoUpdaterConfig.getFluoZookeepers().get());

        // Accumulo Fluo Table configuration values.
        fluoClientConfig.setAccumuloZookeepers(fluoUpdaterConfig.getAccumuloZookeepers().get());
        fluoClientConfig.setAccumuloInstance(fluoUpdaterConfig.getAccumuloInstance().get());
        fluoClientConfig.setAccumuloUser(fluoUpdaterConfig.getAccumuloUsername().get());
        fluoClientConfig.setAccumuloPassword(fluoUpdaterConfig.getAccumuloPassword().get());

        return FluoFactory.newClient(fluoClientConfig);
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
