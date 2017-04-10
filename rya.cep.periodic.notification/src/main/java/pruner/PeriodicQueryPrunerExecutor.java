package pruner;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_INSTANCE;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_PASSWORD;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_USERNAME;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.ACCUMULO_ZOOKEEPERS;
import static org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig.FLUO_APP_NAME;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.fluo.FluoPcjUpdaterConfig;

import com.google.common.base.Preconditions;

import api.LifeCycle;
import api.NodeBin;

public class PeriodicQueryPrunerExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(PeriodicQueryPrunerExecutor.class);
    private AccumuloRdfConfiguration conf;
    private int numThreads;
    private ExecutorService executor;
    private BlockingQueue<NodeBin> bins;
    
    public PeriodicQueryPrunerExecutor(AccumuloRdfConfiguration conf, int numThreads, BlockingQueue<NodeBin> bins) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkArgument(numThreads > 0);
        this.conf = conf;
        this.numThreads = numThreads;
        executor = Executors.newFixedThreadPool(numThreads);
        this.bins = bins;
    }
    
    @Override
    public void start() {
        try {
            AccumuloBinPruner accPruner = this.getAccumuloPrunerFromConfig(conf);
            FluoClient client = this.getFluoClientFromConfig(conf);
            FluoBinPruner fluoPruner = new FluoBinPruner(client);
            for(int threadNumber = 0; threadNumber < numThreads; threadNumber ++ ) {
                executor.submit(new PeriodicQueryPruner(fluoPruner, accPruner, client, bins, threadNumber));
            }
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.info("Unable to cleanly initialize the Executor.  Could not connect to the Accumulo table.");
            throw new RuntimeException(e);
        }

    }

    @Override
    public void stop() {
        if (executor != null)
            executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    private AccumuloBinPruner getAccumuloPrunerFromConfig(AccumuloRdfConfiguration conf) throws AccumuloException, AccumuloSecurityException {
        Authorizations auths = ConfigUtils.getAuthorizations(conf);
        Connector connector = ConfigUtils.getConnector(conf);
        String ryaInstanceName = conf.getTablePrefix();
        return new AccumuloBinPruner(connector, auths, ryaInstanceName);
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
    
}
