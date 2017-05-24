package org.apache.rya.periodic.notification.pruner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;

import com.google.common.base.Preconditions;

public class PeriodicQueryPrunerExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(PeriodicQueryPrunerExecutor.class);
    private AccumuloRdfConfiguration conf;
    private FluoClient client;
    private int numThreads;
    private ExecutorService executor;
    private BlockingQueue<NodeBin> bins;
    private boolean running = false;

    public PeriodicQueryPrunerExecutor(AccumuloRdfConfiguration conf, FluoClient client, int numThreads, BlockingQueue<NodeBin> bins) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkArgument(numThreads > 0);
        this.conf = conf;
        this.numThreads = numThreads;
        executor = Executors.newFixedThreadPool(numThreads);
        this.bins = bins;
        this.client = client;
    }

    @Override
    public void start() {
        try {
            AccumuloBinPruner accPruner = this.getAccumuloPrunerFromConfig(conf);
            FluoBinPruner fluoPruner = new FluoBinPruner(client);
            for (int threadNumber = 0; threadNumber < numThreads; threadNumber++) {
                executor.submit(new PeriodicQueryPruner(fluoPruner, accPruner, client, bins, threadNumber));
            }
            running = true;
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.info("Unable to cleanly initialize the Executor.  Could not connect to the Accumulo table.");
            throw new RuntimeException(e);
        }

    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            running = false;
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    private AccumuloBinPruner getAccumuloPrunerFromConfig(AccumuloRdfConfiguration conf)
            throws AccumuloException, AccumuloSecurityException {
        Connector connector = ConfigUtils.getConnector(conf);
        String ryaInstanceName = conf.getTablePrefix();
        
        return new AccumuloBinPruner(new AccumuloPeriodicQueryResultStorage(connector, ryaInstanceName));
    }


    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
