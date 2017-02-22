package cep.periodic.notification.exporter;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.PeriodicBinUpdater;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;

import jline.internal.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import rya.cep.periodic.api.BindingSetExporter;

public class KafkaPeriodicBindingSetExporter implements BindingSetExporter, Runnable {

    private static final Logger log = Logger.getLogger(BindingSetExporter.class);
    private String topic;
    private Producer<String, BindingSet> producer;
    private BlockingQueue<BindingSet> bindingSets;
    private int threadNumber;

    public KafkaPeriodicBindingSetExporter(String topic, Producer<String, BindingSet> producer, int threadNumber,
            BlockingQueue<BindingSet> bindingSets) {
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(producer);
        Preconditions.checkNotNull(bindingSets);
        this.topic = topic;
        this.threadNumber = threadNumber;
        this.producer = producer;
        this.bindingSets = bindingSets;
    }

    @Override
    public void exportNotification(BindingSet bindingSet) {
        String bindingName = PeriodicBinUpdater.BIN_ID;
        long binId = ((Literal) bindingSet.getValue(bindingName)).longValue();
        producer.send(new KeyedMessage<String, BindingSet>(topic, Long.toString(binId), bindingSet));
    }

    @Override
    public void run() {
        try {
           exportNotification(bindingSets.take());
        } catch (InterruptedException e) {
            log.trace("Thread " + threadNumber + " is unable to process message.");
        }
    }

}
