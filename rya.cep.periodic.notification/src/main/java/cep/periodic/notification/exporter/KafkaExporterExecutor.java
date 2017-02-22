package cep.periodic.notification.exporter;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;

import cep.periodic.notification.serialization.BindingSetEncoder;
import jline.internal.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import rya.cep.periodic.api.BindingSetExporter;
import rya.cep.periodic.api.LifeCycle;

public class KafkaExporterExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(BindingSetExporter.class);
    private Properties props;
    private String topic;
    private Producer<String, BindingSet> producer;
    private BlockingQueue<BindingSet> bindingSets;
    private ExecutorService executor;
    private int num_Threads;
    private boolean running = false;
    
    public KafkaExporterExecutor(String topic, Properties props, int num_Threads, BlockingQueue<BindingSet> bindingSets) {
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(props);
        this.props = props;
        this.topic = topic;
        this.bindingSets = bindingSets;
        this.num_Threads = num_Threads;
    }

    private void init(Properties props) {
        props.setProperty("key.serializer.class", StringEncoder.class.getName());
        props.setProperty("serializer.class", BindingSetEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<String, BindingSet>(producerConfig);
    }
    
    @Override
    public void start() {
        init(props);
        executor = Executors.newFixedThreadPool(num_Threads);
        
        for (int threadNumber = 0; threadNumber < num_Threads; threadNumber++) {
            log.info("Creating exporter:" + threadNumber);
            executor.submit(new KafkaPeriodicBindingSetExporter(topic, producer, threadNumber, bindingSets));
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
