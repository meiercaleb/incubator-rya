package cep.periodic.notification.processor;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.data.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

import cep.periodic.notification.notification.TimestampedNotification;
import rya.cep.periodic.api.NodeBin;
import rya.cep.periodic.api.NotificationProcessor;

public class TimestampedNotificationProcessor implements NotificationProcessor, Runnable {

    private static final Logger log = Logger.getLogger(TimestampedNotificationProcessor.class);
    private static final PcjTableNameFactory pcjTableNameFactory = new PcjTableNameFactory();
    private static final AccumuloPcjSerializer serializer = new AccumuloPcjSerializer();
    private BlockingQueue<TimestampedNotification> notifications; // notifications
                                                                  // to process
    private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
    private BlockingQueue<BindingSet> bindingSets; // query results to export
    private AccumuloRdfConfiguration conf;
    private String ryaInstance;
    private Authorizations auths;
    private Connector conn;
    private int threadNumber;
    private FluoClient client;

    public TimestampedNotificationProcessor(FluoClient client, AccumuloRdfConfiguration conf,
            BlockingQueue<TimestampedNotification> notifications, BlockingQueue<NodeBin> bins, BlockingQueue<BindingSet> bindingSets,
            int threadNumber) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkNotNull(notifications);
        Preconditions.checkNotNull(bins);
        Preconditions.checkNotNull(bindingSets);
        Preconditions.checkNotNull(client);
        this.notifications = notifications;
        this.bins = bins;
        this.bindingSets = bindingSets;
        this.conf = conf;
        this.threadNumber = threadNumber;
        this.client = client;
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

        try (Snapshot sx = client.newSnapshot()) {
            String pcjId = sx.get(Bytes.of(id), FluoQueryColumns.RYA_PCJ_ID).toString();
            String varOrderString = sx.get(Bytes.of(id), FluoQueryColumns.QUERY_VARIABLE_ORDER).toString();
            VariableOrder varOrder = new VariableOrder(varOrderString);
            Scanner scanner = conn.createScanner(pcjTableNameFactory.makeTableName(ryaInstance, pcjId), auths);
            scanner.setRange(Range.prefix(new Text(Long.toString(bin))));

            // get all elements in bin that start with ts
            // add them to export queue to be exported
            for (Map.Entry<Key, Value> entry : scanner) {
                Text row = entry.getKey().getRow();
                BindingSet bs = serializer.convert(row.getBytes(), varOrder);
                bindingSets.put(bs);
            }
            // add NodeBin to BinPruner queue so that bin can be deleted from
            // Fluo and Accumulo
            bins.put(nodeBin);
        } catch (TableNotFoundException | BindingSetConversionException | InterruptedException e) {
            log.trace("Thread_" + threadNumber + " is unable to process the notification " + notification);
            throw new RuntimeException(e);
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
            init();
            processNotification(notifications.take());
        } catch (Exception e) {
            log.trace("Thread_" + threadNumber + " is unable to process next notification.");
            throw new RuntimeException(e);
        }

    }

    public static Builder builder() {
        return new Builder();
    }

    private void init() throws AccumuloException, AccumuloSecurityException {
        conn = ConfigUtils.getConnector(conf);
        ryaInstance = conf.getTablePrefix();
        auths = ConfigUtils.getAuthorizations(conf);
    }

    public static class Builder {

        private BlockingQueue<TimestampedNotification> notifications; // notifications to process
        private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
        private BlockingQueue<BindingSet> bindingSets; // query results to export
                                                       
        private AccumuloRdfConfiguration conf;
        private int threadNumber;
        private FluoClient client;

        public Builder setNotifications(BlockingQueue<TimestampedNotification> notifications) {
            this.notifications = notifications;
            return this;
        }

        public Builder setBins(BlockingQueue<NodeBin> bins) {
            this.bins = bins;
            return this;
        }

        public Builder setBindingSets(BlockingQueue<BindingSet> bindingSets) {
            this.bindingSets = bindingSets;
            return this;
        }

        public Builder setConf(AccumuloRdfConfiguration conf) {
            this.conf = conf;
            return this;
        }

        public Builder setThreadNumber(int threadNumber) {
            this.threadNumber = threadNumber;
            return this;
        }

        public Builder setClient(FluoClient client) {
            this.client = client;
            return this;
        }

        public TimestampedNotificationProcessor build() {
            return new TimestampedNotificationProcessor(client, conf, notifications, bins, bindingSets, threadNumber);
        }

    }
}
