package org.apache.rya.periodic.notification.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.exporter.BindingSetRecord;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

public class NotificationProcessorExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(TimestampedNotificationProcessor.class);
    private BlockingQueue<TimestampedNotification> notifications; // notifications
    private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
    private BlockingQueue<BindingSetRecord> bindingSets; // query results to
                                                         // export
    private PeriodicQueryResultStorage periodicStorage;
    private List<TimestampedNotificationProcessor> processors;
    private int numberThreads;
    private ExecutorService executor;
    private boolean running = false;

    public NotificationProcessorExecutor(PeriodicQueryResultStorage periodicStorage, BlockingQueue<TimestampedNotification> notifications,
            BlockingQueue<NodeBin> bins, BlockingQueue<BindingSetRecord> bindingSets, int numberThreads) {
        Preconditions.checkNotNull(notifications);
        Preconditions.checkNotNull(bins);
        Preconditions.checkNotNull(bindingSets);
        this.notifications = notifications;
        this.bins = bins;
        this.bindingSets = bindingSets;
        this.periodicStorage = periodicStorage;
        this.numberThreads = numberThreads;
        processors = new ArrayList<>();
    }

    @Override
    public void start() {
        if (!running) {
            executor = Executors.newFixedThreadPool(numberThreads);
            for (int threadNumber = 0; threadNumber < numberThreads; threadNumber++) {
                log.info("Creating exporter:" + threadNumber);
                TimestampedNotificationProcessor processor = TimestampedNotificationProcessor.builder().setBindingSets(bindingSets)
                        .setBins(bins).setPeriodicStorage(periodicStorage).setNotifications(notifications).setThreadNumber(threadNumber)
                        .build();
                processors.add(processor);
                executor.submit(processor);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (processors != null && processors.size() > 0) {
            processors.forEach(x -> x.shutdown());
        }
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
