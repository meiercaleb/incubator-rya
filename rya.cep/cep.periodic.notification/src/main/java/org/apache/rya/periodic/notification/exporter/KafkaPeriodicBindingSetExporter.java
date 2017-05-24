package org.apache.rya.periodic.notification.exporter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.BindingSetExporter;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter.ResultExportException;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;

import jline.internal.Preconditions;

public class KafkaPeriodicBindingSetExporter implements BindingSetExporter, Runnable {

    private static final Logger log = Logger.getLogger(BindingSetExporter.class);
    private KafkaProducer<String, BindingSet> producer;
    private BlockingQueue<BindingSetRecord> bindingSets;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private int threadNumber;

    public KafkaPeriodicBindingSetExporter(KafkaProducer<String, BindingSet> producer, int threadNumber,
            BlockingQueue<BindingSetRecord> bindingSets) {
        Preconditions.checkNotNull(producer);
        Preconditions.checkNotNull(bindingSets);
        this.threadNumber = threadNumber;
        this.producer = producer;
        this.bindingSets = bindingSets;
    }

    @Override
    public void exportNotification(BindingSetRecord record) throws ResultExportException {
        String bindingName = IncrementalUpdateConstants.PERIODIC_BIN_ID;
        BindingSet bindingSet = record.getBindingSet();
        String topic = record.getTopic();
        long binId = ((Literal) bindingSet.getValue(bindingName)).longValue();
        final Future<RecordMetadata> future = producer
                .send(new ProducerRecord<String, BindingSet>(topic, Long.toString(binId), bindingSet));
        try {
            //wait for confirmation that results have been received
            future.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ResultExportException(e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                exportNotification(bindingSets.take());
            }
        } catch (InterruptedException | ResultExportException e) {
            log.trace("Thread " + threadNumber + " is unable to process message.");
        }
    }
    
    
    public void shutdown() {
        closed.set(true);
    }

}
