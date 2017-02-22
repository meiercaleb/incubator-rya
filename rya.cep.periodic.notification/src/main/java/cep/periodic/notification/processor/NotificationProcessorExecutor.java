package cep.periodic.notification.processor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.FluoClient;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

import cep.periodic.notification.notification.TimestampedNotification;
import rya.cep.periodic.api.LifeCycle;
import rya.cep.periodic.api.NodeBin;

public class NotificationProcessorExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(TimestampedNotificationProcessor.class);
    private BlockingQueue<TimestampedNotification> notifications; // notifications
    private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
    private BlockingQueue<BindingSet> bindingSets; // query results to export
    private AccumuloRdfConfiguration conf;
    private int numberThreads;
    private FluoClient client;
    private ExecutorService executor;
    private boolean running = false;

    public NotificationProcessorExecutor(FluoClient client, AccumuloRdfConfiguration conf,
            BlockingQueue<TimestampedNotification> notifications, BlockingQueue<NodeBin> bins, BlockingQueue<BindingSet> bindingSets,
            int numberThreads) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkNotNull(notifications);
        Preconditions.checkNotNull(bins);
        Preconditions.checkNotNull(bindingSets);
        Preconditions.checkNotNull(client);
        this.notifications = notifications;
        this.bins = bins;
        this.bindingSets = bindingSets;
        this.conf = conf;
        this.numberThreads = numberThreads;
        this.client = client;
    }

    @Override
    public void start() {
        executor = Executors.newFixedThreadPool(numberThreads);

        for (int threadNumber = 0; threadNumber < numberThreads; threadNumber++) {
            log.info("Creating exporter:" + threadNumber);
            executor.submit(TimestampedNotificationProcessor.builder().setBindingSets(bindingSets).setBins(bins).setClient(client)
                    .setConf(conf).setNotifications(notifications).setThreadNumber(threadNumber).build());
        }
        running = true;
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
        }
        running = false;
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }
    
    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
