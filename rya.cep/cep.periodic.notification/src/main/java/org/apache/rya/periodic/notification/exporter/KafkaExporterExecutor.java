package org.apache.rya.periodic.notification.exporter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.BindingSetExporter;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.openrdf.query.BindingSet;

import jline.internal.Preconditions;

public class KafkaExporterExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(BindingSetExporter.class);
    private KafkaProducer<String, BindingSet> producer;
    private BlockingQueue<BindingSetRecord> bindingSets;
    private ExecutorService executor;
    private List<KafkaPeriodicBindingSetExporter> exporters;
    private int num_Threads;
    private boolean running = false;

    public KafkaExporterExecutor(KafkaProducer<String, BindingSet> producer, int num_Threads, BlockingQueue<BindingSetRecord> bindingSets) {
        Preconditions.checkNotNull(producer);
        Preconditions.checkNotNull(bindingSets);
        this.producer = producer;
        this.bindingSets = bindingSets;
        this.num_Threads = num_Threads;
        this.exporters = new ArrayList<>();
    }

    @Override
    public void start() {
        if (!running) {
            executor = Executors.newFixedThreadPool(num_Threads);

            for (int threadNumber = 0; threadNumber < num_Threads; threadNumber++) {
                log.info("Creating exporter:" + threadNumber);
                KafkaPeriodicBindingSetExporter exporter = new KafkaPeriodicBindingSetExporter(producer, threadNumber, bindingSets);
                exporters.add(exporter);
                executor.submit(exporter);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
        }

        if (exporters != null && exporters.size() > 0) {
            exporters.forEach(x -> x.shutdown());
        }

        if (producer != null) {
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
