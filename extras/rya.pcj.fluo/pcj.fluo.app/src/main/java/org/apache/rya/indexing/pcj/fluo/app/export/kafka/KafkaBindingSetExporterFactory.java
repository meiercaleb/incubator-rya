/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import org.apache.fluo.api.observer.Observer.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import com.google.common.base.Optional;

/**
 * Creates instances of {@link KafkaBindingSetExporter}.
 * <p/>
 * Configure a Kafka producer by adding several required Key/values as described here:
 * http://kafka.apache.org/documentation.html#producerconfigs
 * <p/>
 * Here is a simple example:
 * <pre>
 *     Properties producerConfig = new Properties();
 *     producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *     producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
 *     producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
 * </pre>
 * 
 * @see ProducerConfig
 */
public class KafkaBindingSetExporterFactory implements IncrementalBindingSetExporterFactory {
    private static final Logger log = Logger.getLogger(KafkaBindingSetExporterFactory.class);
    @Override
    public Optional<IncrementalBindingSetExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        final KafkaExportParameters exportParams = new KafkaExportParameters(context.getObserverConfiguration().toMap());
        log.debug("KafkaResultExporterFactory.build(): params.isExportToKafka()=" + exportParams.isExportToKafka());
        if (exportParams.isExportToKafka()) {
            // Setup Kafka connection
            KafkaProducer<String, VisibilityBindingSet> producer = new KafkaProducer<String, VisibilityBindingSet>(exportParams.listAllConfig());
            // Create the exporter
            final IncrementalBindingSetExporter exporter = new KafkaBindingSetExporter(producer);
            return Optional.of(exporter);
        } else {
            return Optional.absent();
        }
    }

}
