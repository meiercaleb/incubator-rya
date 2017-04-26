package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import org.apache.fluo.api.observer.Observer.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.ConfigurationException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporterFactory;

import com.google.common.base.Optional;

public class KafkaRyaSubGraphExporterFactory implements IncrementalRyaSubGraphExporterFactory {

    private static final Logger log = Logger.getLogger(KafkaRyaSubGraphExporterFactory.class);
    
    @Override
    public Optional<IncrementalRyaSubGraphExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        final KafkaExportParameters exportParams = new KafkaExportParameters(context.getObserverConfiguration().toMap());
        log.debug("KafkaRyaSubGraphExporterFactory.build(): params.isExportToKafka()=" + exportParams.isExportToKafka());
        if (exportParams.isExportToKafka()) {
            // Setup Kafka connection
            KafkaProducer<String, RyaSubGraph> producer = new KafkaProducer<String, RyaSubGraph>(exportParams.listAllConfig());
            // Create the exporter
            final IncrementalRyaSubGraphExporter exporter = new KafkaRyaSubGraphExporter(producer);
            return Optional.of(exporter);
        } else {
            return Optional.absent();
        }
    }

}
