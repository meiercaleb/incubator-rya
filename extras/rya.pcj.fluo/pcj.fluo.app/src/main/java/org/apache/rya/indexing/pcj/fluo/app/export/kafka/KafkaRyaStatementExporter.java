package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;

import jline.internal.Preconditions;

import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaStatementExporter;

public class KafkaRyaStatementExporter implements IncrementalRyaStatementExporter {

    private final KafkaProducer<String, RyaStatement> producer;
    private static final Logger log = Logger.getLogger(KafkaRyaStatementExporter.class);

    public KafkaRyaStatementExporter(KafkaProducer producer) {
        Preconditions.checkNotNull(producer);
        this.producer = producer;
    }
    
    @Override
    public void export(String constructID, RyaStatement statement) throws ResultExportException {
        
    }

}
