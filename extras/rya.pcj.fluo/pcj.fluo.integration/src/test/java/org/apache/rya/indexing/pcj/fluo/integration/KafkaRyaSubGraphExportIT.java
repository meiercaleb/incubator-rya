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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.fluo.KafkaExportITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerDe;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Sets;

public class KafkaRyaSubGraphExportIT extends KafkaExportITBase {

    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private Map<String, Long> timeStampMap;

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
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RyaSubGraphKafkaSerDe.class.getName());
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
        final String pcjId = loadStatements(sparql, statements);

        // Verify the end results of the query match the expected results.
        final Set<RyaSubGraph> results = readAllResults(pcjId);
        setTimeStampMap(results);
        // Create the expected results of the SPARQL query once the PCJ has been
        // computed.
        final Set<RyaSubGraph> expectedResults = new HashSet<>();
        RyaSubGraph subGraph = new RyaSubGraph(pcjId);
        RyaStatement statement1 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:travelsTo"), new RyaURI("urn:London"));
        RyaStatement statement2 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:friendsWith"), new RyaURI("urn:Bob"));
        // if no visibility indicated, then visibilities set to empty byte in
        // Fluo - they are null by default in RyaStatement
        // need to set visibility to empty byte so that RyaStatement's equals
        // will return true
        statement1.setColumnVisibility(new byte[0]);
        statement2.setColumnVisibility(new byte[0]);

        Set<RyaStatement> stmnts = new HashSet<>(Arrays.asList(statement1, statement2));
        stmnts.forEach(x -> setTimeStamp(x));
        subGraph.setStatements(stmnts);
        expectedResults.add(subGraph);

        assertEquals(expectedResults, results);
    }

    @Test
    public void basicConstructQueryWithVis() throws Exception {
        // A query that groups what is aggregated by one of the keys.
        final String sparql = "CONSTRUCT { ?customer <urn:travelsTo> ?city . ?customer <urn:friendsWith> ?worker }" + "WHERE { "
                + "?customer <urn:talksTo> ?worker. " + "?worker <urn:livesIn> ?city. " + "?worker <urn:worksAt> <urn:burgerShack>. " + "}";

        // Create the Statements that will be loaded into Rya.
        RyaStatement statement1 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:talksTo"), new RyaURI("urn:Bob"));
        RyaStatement statement2 = new RyaStatement(new RyaURI("urn:Bob"), new RyaURI("urn:livesIn"), new RyaURI("urn:London"));
        RyaStatement statement3 = new RyaStatement(new RyaURI("urn:Bob"), new RyaURI("urn:worksAt"), new RyaURI("urn:burgerShack"));
        statement1.setColumnVisibility("U&W".getBytes("UTF-8"));
        statement2.setColumnVisibility("V".getBytes("UTF-8"));
        statement3.setColumnVisibility("W".getBytes("UTF-8"));
        
        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadRyaStatements(sparql, Arrays.asList(statement1, statement2, statement3));

        // Verify the end results of the query match the expected results.
        final Set<RyaSubGraph> results = readAllResults(pcjId);
        setTimeStampMap(results);
        // Create the expected results of the SPARQL query once the PCJ has been
        // computed.
        final Set<RyaSubGraph> expectedResults = new HashSet<>();
        RyaSubGraph subGraph = new RyaSubGraph(pcjId);
        RyaStatement statement4 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:travelsTo"), new RyaURI("urn:London"));
        RyaStatement statement5 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:friendsWith"), new RyaURI("urn:Bob"));
        // if no visibility indicated, then visibilities set to empty byte in
        // Fluo - they are null by default in RyaStatement
        // need to set visibility to empty byte so that RyaStatement's equals
        // will return true
        statement4.setColumnVisibility("U&V&W".getBytes("UTF-8"));
        statement5.setColumnVisibility("U&V&W".getBytes("UTF-8"));

        Set<RyaStatement> stmnts = new HashSet<>(Arrays.asList(statement4, statement5));
        stmnts.forEach(x -> setTimeStamp(x));
        subGraph.setStatements(stmnts);
        expectedResults.add(subGraph);

        assertEquals(expectedResults, results);
    }

    
    @Test
    public void constructQueryWithVisAndMultipleSubGraphs() throws Exception {
        // A query that groups what is aggregated by one of the keys.
        final String sparql = "CONSTRUCT { ?customer <urn:travelsTo> ?city . ?customer <urn:friendsWith> ?worker }" + "WHERE { "
                + "?customer <urn:talksTo> ?worker. " + "?worker <urn:livesIn> ?city. " + "?worker <urn:worksAt> <urn:burgerShack>. " + "}";

        // Create the Statements that will be loaded into Rya.
        RyaStatement statement1 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:talksTo"), new RyaURI("urn:Bob"));
        RyaStatement statement2 = new RyaStatement(new RyaURI("urn:Bob"), new RyaURI("urn:livesIn"), new RyaURI("urn:London"));
        RyaStatement statement3 = new RyaStatement(new RyaURI("urn:Bob"), new RyaURI("urn:worksAt"), new RyaURI("urn:burgerShack"));
        RyaStatement statement4 = new RyaStatement(new RyaURI("urn:John"), new RyaURI("urn:talksTo"), new RyaURI("urn:Evan"));
        RyaStatement statement5 = new RyaStatement(new RyaURI("urn:Evan"), new RyaURI("urn:livesIn"), new RyaURI("urn:SanFrancisco"));
        RyaStatement statement6 = new RyaStatement(new RyaURI("urn:Evan"), new RyaURI("urn:worksAt"), new RyaURI("urn:burgerShack"));
        statement1.setColumnVisibility("U&W".getBytes("UTF-8"));
        statement2.setColumnVisibility("V".getBytes("UTF-8"));
        statement3.setColumnVisibility("W".getBytes("UTF-8"));
        statement4.setColumnVisibility("A&B".getBytes("UTF-8"));
        statement5.setColumnVisibility("B".getBytes("UTF-8"));
        statement6.setColumnVisibility("C".getBytes("UTF-8"));
        
        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadRyaStatements(sparql, Arrays.asList(statement1, statement2, statement3, statement4, statement5, statement6));

        // Verify the end results of the query match the expected results.
        final Set<RyaSubGraph> results = readAllResults(pcjId);
        setTimeStampMap(results);
        // Create the expected results of the SPARQL query once the PCJ has been
        // computed.
        RyaStatement statement7 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:travelsTo"), new RyaURI("urn:London"));
        RyaStatement statement8 = new RyaStatement(new RyaURI("urn:Joe"), new RyaURI("urn:friendsWith"), new RyaURI("urn:Bob"));
        RyaStatement statement9 = new RyaStatement(new RyaURI("urn:John"), new RyaURI("urn:travelsTo"), new RyaURI("urn:SanFrancisco"));
        RyaStatement statement10 = new RyaStatement(new RyaURI("urn:John"), new RyaURI("urn:friendsWith"), new RyaURI("urn:Evan"));
        statement7.setColumnVisibility("U&V&W".getBytes("UTF-8"));
        statement8.setColumnVisibility("U&V&W".getBytes("UTF-8"));
        statement9.setColumnVisibility("A&B&C".getBytes("UTF-8"));
        statement10.setColumnVisibility("A&B&C".getBytes("UTF-8"));

        final Set<RyaSubGraph> expectedResults = new HashSet<>();

        RyaSubGraph subGraph1 = new RyaSubGraph(pcjId);
        Set<RyaStatement> stmnts1 = new HashSet<>(Arrays.asList(statement7, statement8));
        stmnts1.forEach(x -> setTimeStamp(x));
        subGraph1.setStatements(stmnts1);
        expectedResults.add(subGraph1);
        
        RyaSubGraph subGraph2 = new RyaSubGraph(pcjId);
        Set<RyaStatement> stmnts2 = new HashSet<>(Arrays.asList(statement9, statement10));
        stmnts2.forEach(x -> setTimeStamp(x));
        subGraph2.setStatements(stmnts2);
        expectedResults.add(subGraph2);

        assertEquals(expectedResults, results);
    }
    
    protected KafkaConsumer<String, RyaSubGraph> makeRyaSubGraphConsumer(final String TopicName) {
        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RyaSubGraphKafkaSerDe.class.getName());

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
    
    protected String loadStatements(final String sparql, final Collection<Statement> statements) throws Exception {
        return loadRyaStatements(sparql, statements.stream().map(x -> RdfToRyaConversions.convertStatement(x)).collect(Collectors.toSet()));
    }
    

    protected String loadRyaStatements(final String sparql, final Collection<RyaStatement> statements) throws Exception {
        requireNonNull(sparql);
        requireNonNull(statements);
        FluoClient client = null;

        try {
            CreatePcj createPcj = new CreatePcj();
            client = new FluoClientImpl(super.getFluoConfiguration());
            FluoQuery fluoQuery = createPcj.createFluoPcj(client, sparql);

            AccumuloRyaDAO dao = getRyaDAO();
            dao.add(statements.iterator());

            // Wait for the Fluo application to finish computing the end result.
            super.getMiniFluo().waitForObservers();

            // FluoITHelper.printFluoTable(client);
            return fluoQuery.getConstructQueryMetadata().get().getNodeId();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private RyaStatement setTimeStamp(RyaStatement statement) {
        String key = getRyaStatementKey(statement);
        Long ts = timeStampMap.get(key);
        if (ts == null) {
            throw new RuntimeException("RyaStatement not in timestamp map!");
        }
        statement.setTimestamp(ts);
        return statement;
    }

    private void setTimeStampMap(Set<RyaSubGraph> subgraphs) {
        timeStampMap = new HashMap<>();
        for (RyaSubGraph subgraph : subgraphs) {
            for (RyaStatement statement : subgraph.getStatements()) {
                timeStampMap.put(getRyaStatementKey(statement), statement.getTimestamp());
            }
        }
    }

    private String getRyaStatementKey(RyaStatement statement) {
        return new StringBuilder().append(statement.getSubject().getData()).append(";").append(statement.getPredicate().getData()).append(";")
                .append(statement.getObject().getData()).toString();
    }

}
