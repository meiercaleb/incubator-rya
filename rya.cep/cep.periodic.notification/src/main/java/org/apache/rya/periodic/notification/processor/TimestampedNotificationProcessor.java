package org.apache.rya.periodic.notification.processor;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.cep.periodic.api.NotificationProcessor;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.periodic.notification.exporter.BindingSetRecord;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

public class TimestampedNotificationProcessor implements NotificationProcessor, Runnable {

    private static final Logger log = Logger.getLogger(TimestampedNotificationProcessor.class);
    private PeriodicQueryResultStorage periodicStorage;
    private BlockingQueue<TimestampedNotification> notifications; // notifications
                                                                  // to process
    private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
    private BlockingQueue<BindingSetRecord> bindingSets; // query results to export
    private AtomicBoolean closed = new AtomicBoolean(false);
    private int threadNumber;

    public TimestampedNotificationProcessor(PeriodicQueryResultStorage periodicStorage,
            BlockingQueue<TimestampedNotification> notifications, BlockingQueue<NodeBin> bins, BlockingQueue<BindingSetRecord> bindingSets,
            int threadNumber) {
        Preconditions.checkNotNull(notifications);
        Preconditions.checkNotNull(bins);
        Preconditions.checkNotNull(bindingSets);
        this.periodicStorage = periodicStorage;
        this.notifications = notifications;
        this.bins = bins;
        this.bindingSets = bindingSets;
        this.threadNumber = threadNumber;
    }

    /**
     * Processes the TimestampNotifications by scanning the PCJ tables for
     * entries in the bin corresponding to
     * {@link TimestampedNotification#getTimestamp()} and adding them to the
     * export BlockingQueue. The TimestampNotification is then used to form a
     * {@link NodeBin} that is passed to the BinPruner BlockingQueue so that the
     * bins can be deleted from Fluo and Accumulo.
     */
    @Override
    public void processNotification(TimestampedNotification notification) {

        String id = notification.getId();
        long ts = notification.getTimestamp().getTime();
        long period = notification.getPeriod();
        long bin = getBinFromTimestamp(ts, period);
        NodeBin nodeBin = new NodeBin(id, bin);

        try (CloseableIterator<BindingSet> iter = periodicStorage.listResults(id, Optional.of(bin));) {

            while(iter.hasNext()) {
                bindingSets.add(new BindingSetRecord(iter.next(), id));
            }
            // add NodeBin to BinPruner queue so that bin can be deleted from
            // Fluo and Accumulo
            bins.put(nodeBin);
        } catch (Exception e) {
            log.debug("Encountered error: " + e.getMessage() + " while accessing periodic results for bin: " + bin + " for query: " + id);
        }
    }

    /**
     * Computes left bin end point containing event time ts
     * 
     * @param ts
     *            - event time
     * @param start
     *            - time that periodic event began
     * @param period
     *            - length of period
     * @return left bin end point containing event time ts
     */
    private long getBinFromTimestamp(long ts, long period) {
        Preconditions.checkArgument(period > 0);
        return (ts / period) * period;
    }

    @Override
    public void run() {
        try {
            while(!closed.get()) {
                processNotification(notifications.take());
            }
        } catch (Exception e) {
            log.trace("Thread_" + threadNumber + " is unable to process next notification.");
            throw new RuntimeException(e);
        }

    }
    
    public void shutdown() {
        closed.set(true);
    }

    public static Builder builder() {
        return new Builder();
    }

  

    public static class Builder {

        private PeriodicQueryResultStorage periodicStorage;
        private BlockingQueue<TimestampedNotification> notifications; // notifications to process
        private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
        private BlockingQueue<BindingSetRecord> bindingSets; // query results to export
                                                       
        private int threadNumber;

        public Builder setNotifications(BlockingQueue<TimestampedNotification> notifications) {
            this.notifications = notifications;
            return this;
        }

        public Builder setBins(BlockingQueue<NodeBin> bins) {
            this.bins = bins;
            return this;
        }

        public Builder setBindingSets(BlockingQueue<BindingSetRecord> bindingSets) {
            this.bindingSets = bindingSets;
            return this;
        }

        public Builder setThreadNumber(int threadNumber) {
            this.threadNumber = threadNumber;
            return this;
        }
        
        public Builder setPeriodicStorage(PeriodicQueryResultStorage periodicStorage) {
            this.periodicStorage = periodicStorage;
            return this;
        }

        public TimestampedNotificationProcessor build() {
            return new TimestampedNotificationProcessor(periodicStorage, notifications, bins, bindingSets, threadNumber);
        }

    }
}
