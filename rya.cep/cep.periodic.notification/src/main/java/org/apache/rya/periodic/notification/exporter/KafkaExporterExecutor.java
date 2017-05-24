package org.apache.rya.periodic.notification.exporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.BindingSetExporter;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.openrdf.query.BindingSet;

import jline.internal.Preconditions;

public class KafkaExporterExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(BindingSetExporter.class);
    private Properties props;
    private KafkaProducer<String, BindingSet> producer;
    private BlockingQueue<BindingSetRecord> bindingSets;
    private ExecutorService executor;
    private List<KafkaPeriodicBindingSetExporter> exporters;
    private int num_Threads;
    private boolean running = false;
    
    public KafkaExporterExecutor(Properties props, int num_Threads, BlockingQueue<BindingSetRecord> bindingSets) {
        Preconditions.checkNotNull(props);
        Preconditions.checkNotNull(bindingSets);
        this.props = props;
        this.bindingSets = bindingSets;
        this.num_Threads = num_Threads;
        this.exporters = new ArrayList<>();
    }

    private void init(Properties props) {
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BindingSetSerDe.class.getName());
        producer = new KafkaProducer<String, BindingSet>(props);
    }
    
    @Override
    public void start() {
        init(props);
        executor = Executors.newFixedThreadPool(num_Threads);
        
        for (int threadNumber = 0; threadNumber < num_Threads; threadNumber++) {
            log.info("Creating exporter:" + threadNumber);
            KafkaPeriodicBindingSetExporter exporter = new KafkaPeriodicBindingSetExporter(producer, threadNumber, bindingSets);
            exporters.add(exporter);
            executor.submit(exporter);
        }
        running = true;
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
        }
        
        if(exporters != null && exporters.size() > 0) {
            exporters.forEach(x -> x.shutdown());
        }
        
        if(producer != null) {
            producer.close();
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
