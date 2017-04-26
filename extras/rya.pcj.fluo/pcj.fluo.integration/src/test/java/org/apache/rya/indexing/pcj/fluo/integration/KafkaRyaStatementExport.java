package org.apache.rya.indexing.pcj.fluo.integration;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.KafkaExportITBase;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerializer;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Sets;

public class KafkaRyaStatementExport extends KafkaExportITBase {

    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";

    /**
     * Add info about the Kafka queue/topic to receive the export.
     *
     * @see org.apache.rya.indexing.pcj.fluo.ITBase#setExportParameters(java.util.HashMap)
     */
    @Override
    protected void preFluoInitHook() throws Exception {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();
        observers.add(new ObserverSpecification(TripleObserver.class.getName()));
        observers.add(new ObserverSpecification(StatementPatternObserver.class.getName()));
        observers.add(new ObserverSpecification(JoinObserver.class.getName()));
        observers.add(new ObserverSpecification(FilterObserver.class.getName()));
        observers.add(new ObserverSpecification(AggregationObserver.class.getName()));

        // Configure the export observer to export new PCJ results to the mini
        // accumulo cluster.
        final HashMap<String, String> exportParams = new HashMap<>();

        final KafkaExportParameters kafkaParams = new KafkaExportParameters(exportParams);
        kafkaParams.setExportToKafka(true);

        // Configure the Kafka Producer
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RyaSubGraphKafkaSerializer.class.getName());
        kafkaParams.addAllProducerConfig(producerConfig);

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(ConstructQueryResultObserver.class.getName(),
                exportParams);
        observers.add(exportObserverConfig);

        // Add the observers to the Fluo Configuration.
        super.getFluoConfiguration().addObservers(observers);
    }

    @Test
    public void basicConstructQuery() throws Exception {
        // A query that groups what is aggregated by one of the keys.
        final String sparql = "CONSTRUCT { ?customer <urn:travelsTo> ?city . ?customer <urn:friendsWith> ?worker }" + "WHERE { "
                + "?customer <urn:talksTo> ?worker. " + "?worker <urn:livesIn> ?city. " + "?worker <urn:worksAt> <urn:burgerShack>. " + "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:Joe"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")),
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:livesIn"), vf.createURI("urn:London")),
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:burgerShack")));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadData(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been
        // computed.
        final Set<RyaSubGraph> expectedResults = new HashSet<>();

        RyaSubGraph subGraph = new RyaSubGraph("pcjId");
        List<RyaStatement> stmnts = Arrays.asList(
                new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:travelsTo"), new RyaURI("urn:London")),
                new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:friendsWith"), new RyaURI("urn:Bob")));
        subGraph.setStatements(stmnts);
        expectedResults.add(subGraph);

        // Verify the end results of the query match the expected results.
        final Set<RyaSubGraph> results = readAllResults(pcjId);
        assertEquals(expectedResults, results);
    }

    protected KafkaConsumer<String, RyaSubGraph> makeRyaSubGraphConsumer(final String TopicName) {
        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RyaSubGraphKafkaSerializer.class.getName());

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, RyaSubGraph> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TopicName));
        return consumer;
    }

    private Set<RyaSubGraph> readAllResults(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read all of the results from the Kafka topic.
        final Set<RyaSubGraph> results = new HashSet<>();

        try (final KafkaConsumer<String, RyaSubGraph> consumer = makeRyaSubGraphConsumer(pcjId)) {
            final ConsumerRecords<String, RyaSubGraph> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, RyaSubGraph>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                results.add(recordIterator.next().value());
            }
        }

        return results;
    }

    private RyaSubGraph readLastResult(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read the results from the Kafka topic. The last one has the final
        // aggregation result.
        RyaSubGraph result = null;

        try (final KafkaConsumer<String, RyaSubGraph> consumer = makeRyaSubGraphConsumer(pcjId)) {
            final ConsumerRecords<String, RyaSubGraph> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, RyaSubGraph>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                result = recordIterator.next().value();
            }
        }

        return result;
    }

}
