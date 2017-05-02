package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;

import jline.internal.Preconditions;

public class KafkaRyaSubGraphExporter implements IncrementalRyaSubGraphExporter {

    private final KafkaProducer<String, RyaSubGraph> producer;
    private static final Logger log = Logger.getLogger(KafkaRyaSubGraphExporter.class);

    public KafkaRyaSubGraphExporter(KafkaProducer<String, RyaSubGraph> producer) {
        Preconditions.checkNotNull(producer);
        this.producer = producer;
    }
    
    @Override
    public void export(String constructID, RyaSubGraph subGraph) throws ResultExportException {
        checkNotNull(constructID);
        checkNotNull(subGraph);
        try {
            // Send the result to the topic whose name matches the PCJ ID.
            final ProducerRecord<String, RyaSubGraph> rec = new ProducerRecord<>(subGraph.getId(), subGraph);
            final Future<RecordMetadata> future = producer.send(rec);

            // Don't let the export return until the result has been written to the topic. Otherwise we may lose results.
            future.get();

            log.debug("Producer successfully sent record with id: " + constructID + " and statements: " + subGraph.getStatements());

        } catch (final Throwable e) {
            throw new ResultExportException("A result could not be exported to Kafka.", e);
        }
    }

    @Override
    public void close() throws Exception {
        producer.close(5, TimeUnit.SECONDS);
    }

}
