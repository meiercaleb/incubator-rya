package org.apache.rya.periodic.notification.application;

import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.LifeCycle;
import org.apache.rya.cep.periodic.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.exporter.KafkaExporterExecutor;
import org.apache.rya.periodic.notification.processor.NotificationProcessorExecutor;
import org.apache.rya.periodic.notification.pruner.PeriodicQueryPrunerExecutor;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationProvider;

import com.google.common.base.Preconditions;

public class PeriodicNotificationApplication implements LifeCycle {

    private static final Logger log = Logger.getLogger(PeriodicNotificationApplication.class);
    private NotificationCoordinatorExecutor coordinator;
    private KafkaNotificationProvider provider;
    private PeriodicQueryPrunerExecutor pruner;
    private NotificationProcessorExecutor processor;
    private KafkaExporterExecutor exporter;
    private boolean running = false;

    public PeriodicNotificationApplication(KafkaNotificationProvider provider, NotificationCoordinatorExecutor coordinator,
            NotificationProcessorExecutor processor, KafkaExporterExecutor exporter, PeriodicQueryPrunerExecutor pruner) {
        Preconditions.checkNotNull(provider);
        Preconditions.checkNotNull(coordinator);
        Preconditions.checkNotNull(processor);
        Preconditions.checkNotNull(exporter);
        Preconditions.checkNotNull(pruner);
        this.provider = provider;
        this.coordinator = coordinator;
        this.processor = processor;
        this.exporter = exporter;
        this.pruner = pruner;
    }

    @Override
    public void start() {
        if (!running) {
            log.info("Starting PeriodicNotificationApplication.");
            coordinator.start();
            provider.start();
            processor.start();
            pruner.start();
            exporter.start();
            running = true;
        }
    }

    @Override
    public void stop() {
        log.info("Stopping PeriodicNotificationApplication.");
        provider.stop();
        coordinator.stop();
        processor.stop();
        pruner.stop();
        exporter.stop();
        running = false;
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private PeriodicQueryPrunerExecutor pruner;
        private KafkaNotificationProvider provider;
        private NotificationProcessorExecutor processor;
        private KafkaExporterExecutor exporter;
        private NotificationCoordinatorExecutor coordinator;

        public Builder setPruner(PeriodicQueryPrunerExecutor pruner) {
            this.pruner = pruner;
            return this;
        }

        public Builder setProvider(KafkaNotificationProvider provider) {
            this.provider = provider;
            return this;
        }

        public Builder setProcessor(NotificationProcessorExecutor processor) {
            this.processor = processor;
            return this;
        }

        public Builder setExporter(KafkaExporterExecutor exporter) {
            this.exporter = exporter;
            return this;
        }

        public Builder setCoordinator(NotificationCoordinatorExecutor coordinator) {
            this.coordinator = coordinator;
            return this;
        }

        public PeriodicNotificationApplication build() {
            return new PeriodicNotificationApplication(provider, coordinator, processor, exporter, pruner);
        }

    }

}
