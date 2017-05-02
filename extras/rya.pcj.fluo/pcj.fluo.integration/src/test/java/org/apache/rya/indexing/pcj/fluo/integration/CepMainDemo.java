package org.apache.rya.indexing.pcj.fluo.integration;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.DoubleStream;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.FluoITHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.indexing.pcj.fluo.KafkaExportITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerDe;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.collect.Sets;

public class CepMainDemo extends KafkaExportITBase {
    private static ValueFactory vf = new ValueFactoryImpl();
    
    private static final String event = "http://event/ontology#";
    private static final String vessel = "http://vessel/ontology#";
    private static final String obs = "http://observation/ontology#";
    private static final String country = "http://country/ontology#";
    private static final String geo = "http://www.opengis.net/ont/geosparql#";
    
    private static final URI wktLiteral = vf.createURI("http://www.opengis.net/ont/geosparql#wktLiteral");
    private static final URI vesselMmsi = vf.createURI(vessel +"mmsi");
    private static final URI obsMmsi = vf.createURI(obs + "mmsi");
    private static final URI vesselFlag = vf.createURI(vessel + "flag");
    private static final URI obsWKT = vf.createURI(geo + "asWKT");
    private static final URI obsTime = vf.createURI(event + "time");
    private static final URI countryFlag = vf.createURI(country + "flag");
    private static final URI countryEnemy = vf.createURI(country + "enemy");

    private void init() throws Exception {
        setupMiniAccumulo();
    }

    private void tearDown() {
        try {
            tearDownMiniFluo();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Fluo: " + e.getMessage());
        }
        try {
            teardownKafka();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Kafka: " + e.getMessage());
        }
        try {
            tearDownMiniAccumulo();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Accumulo: " + e.getMessage());
        }
    }

    private void initExample() throws Exception {
        setupMiniFluo();
        setupKafka();
    }

    private void tearDownExample() {
        try {
            teardownRya();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Rya: " + e.getMessage());
        }
        try {
            tearDownMiniFluo();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Fluo: " + e.getMessage());
        }
        try {
            teardownKafka();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Kafka: " + e.getMessage());
        }
    }

    /****************************************************************************
     * Main Method
     ****************************************************************************/
    public static void main(String[] args) {

        CepMainDemo demo = new CepMainDemo();
        try {
            Set<Statement> statements = demo.generateData(100, 100, 20, 20);
            demo.init();
            demo.filterExample(Arrays.asList(statements));
            demo.aggregationExample(Arrays.asList(statements));
            demo.constructQueryExample(Arrays.asList(statements));
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down the application: " + e.getMessage());
        } finally {
            demo.tearDown();
        }
    }

    private void filterExample(List<Set<Statement>> statements) {

      final String sparql = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  \n"//
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  \n"//
              + "PREFIX obs: <http://observation/ontology#>  \n"//
              + "SELECT ?mmsi ?wkt \n" //
              + "{ \n" //
              + "  ?obs obs:mmsi ?mmsi . \n"//
              + "  ?obs geo:asWKT ?wkt . \n"//
              + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) \n" //
              + "} \n";//
      
        System.out.println("**************************************************************************");
        System.out.println("                         RUNNING FILTER EXAMPLE");
        System.out.println("**************************************************************************");
        System.out.println("");
        exampleBase(Optional.empty(), sparql, statements, Optional.empty());

    }

    private void aggregationExample(List<Set<Statement>> statements) {
        
        // point outside search ring
        final String sparql = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  \n"//
                + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  \n"//
                + "PREFIX obs: <http://observation/ontology#>  \n"//
                + "PREFIX vessel: <http://vessel/ontology#>  \n"//
                + "SELECT ?flag (count(?wkt) as ?totalWkt) \n" //
                + "{ \n" //
                + "  ?vessel vessel:flag ?flag . \n"
                + "  ?vessel vessel:mmsi ?mmsi . \n"
                + "  ?obs obs:mmsi ?mmsi . \n"
                + "  ?obs geo:asWKT ?wkt . \n"//
                + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) \n" //
                + "} GROUP BY ?flag \n";//
        
        
        System.out.println("**************************************************************************");
        System.out.println("                         RUNNING AGGREGATION EXAMPLE");
        System.out.println("**************************************************************************");
        System.out.println("");
        exampleBase(Optional.empty(), sparql,statements, Optional.of(new VariableOrder("flag")));
    }

    private void constructQueryExample(List<Set<Statement>> statements) {
        //Define an enemy observation in terms of ontologies
        final String construct = "PREFIX obs: <http://observation/ontology#>  \n"//
                + "PREFIX vessel: <http://vessel/ontology#>  \n"//
                + "PREFIX country: <http://country/ontology#> \n" //
                + "PREFIX base: <http://vessel/observation/> \n" //
                + "PREFIX event: <http://event/ontology#> \n" //
                + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  \n"//
                + "CONSTRUCT { \n" //
                + "_:b a base:EnemyVesselObservation . \n" //
                + "_:b base:observation ?obs . \n"  //
                + "_:b base:vessel ?vessel} \n" //
                + "WHERE { \n" //
                + "?obs event:time ?time . \n" //
                + "?obs geo:asWKT ?wkt . \n" //
                + "?obs obs:mmsi ?mmsi .\n" //
                + "?vessel vessel:mmsi ?mmsi .\n" //
                + "?vessel vessel:flag ?flag .\n" //
                + "?country country:flag ?flag .\n" //
                + "country:UnitedStates country:enemy ?country } \n"; //
               
        //Perform high level aggregations and filtering on semantically enriched triples
        final String aggregation = "PREFIX vessel: <http://vessel/ontology#>  \n"//
                + "PREFIX base: <http://vessel/observation/> \n" //
                + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  \n"//
                + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  \n"//
                + "SELECT ?flag (count(?wkt) as ?totalVessels) { \n"//
                + "?enemyObs a base:EnemyVesselObservation . \n"//
                + "?enemyObs base:observation ?obs . \n"//
                + "?obs geo:asWKT ?wkt . \n"//
                + "?enemyObs base:vessel ?vessel . \n"//
                + "?vessel vessel:flag ?flag .\n"//
                + "FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) \n"//
                + "} GROUP BY ?flag \n";//

        System.out.println("**************************************************************************");
        System.out.println("                         RUNNING CONSTRUCT QUERY EXAMPLE");
        System.out.println("**************************************************************************");
        System.out.println("");
        exampleBase(Optional.of(construct), aggregation, statements, Optional.of(new VariableOrder("flag")));
    }

    private void exampleBase(Optional<String> constructQuery, String sparql, List<Set<Statement>> statements,
            Optional<VariableOrder> groupedVarOrder) {
        try {
            initExample();
            Optional<String> constructPcjId = Optional.empty();
            if (constructQuery.isPresent()) {
                constructPcjId = Optional.of(createConstructPcj(constructQuery.get()));
            }
            String pcjId = createPcj(sparql);
            
            for(Set<Statement> statementSet: statements) {
                loadDataAndPrintResults(statementSet, pcjId, constructPcjId, groupedVarOrder);
            }
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when running an example: " + e.getMessage());
        } finally {
            tearDownExample();
        }
    }
    
    private void loadDataAndPrintResults(Set<Statement> statements, String pcjId, Optional<String> constructPcjId,
            Optional<VariableOrder> groupedVarOrder) throws Exception {
        loadData(statements);
        super.getMiniFluo().waitForObservers();

//        try (FluoClient client = new FluoClientImpl(super.getFluoConfiguration())) {
//            FluoITHelper.printFluoTable(client);
//        }
        
        if(constructPcjId.isPresent()) {
            Set<RyaSubGraph> constructResults = readAllConstructResultsFromKafka(constructPcjId.get());
            printResults(constructResults, constructPcjId.get());
        }
        
        Set<VisibilityBindingSet> results = new HashSet<>();
        if (groupedVarOrder.isPresent()) {
            results = readGroupedResultsFromKafka(pcjId, groupedVarOrder.get());
        } else {
            results = readAllResultsFromKafka(pcjId);
        }

        printResults(results, pcjId);
    }
    
    
    private Set<Statement> generateData(int numAlliesIn, int numEnemiesIn, int numAlliesOut, int numEnemiesOut) {
        
        List<String> allies = Arrays.asList("American", "British", "Austrialian");
        List<String> enemies = Arrays.asList("Russian", "North Korean", "Iranian");
        Random random = new Random();
        
        int baseMMSI = 533100000;
        long currentDate = System.currentTimeMillis();
        DoubleStream allyInStream = random.doubles(2*(numAlliesIn), 0, 1);
        DoubleStream allyOutStream = random.doubles(2*(numAlliesOut), 1, 3);
        DoubleStream enemyInStream = random.doubles(2*(numEnemiesIn), 0, 1);
        DoubleStream enemyOutStream = random.doubles(2*(numEnemiesOut), 1, 3);
        
        Set<Statement> statements = new HashSet<>();
        statements.addAll(buildStatements(numAlliesIn, baseMMSI, allies, currentDate, allyInStream.iterator()));
        baseMMSI += numAlliesIn;
        statements.addAll(buildStatements(numAlliesOut, baseMMSI, allies, currentDate, allyOutStream.iterator()));
        baseMMSI += numAlliesIn;
        statements.addAll(buildStatements(numEnemiesIn, baseMMSI, enemies, currentDate, enemyInStream.iterator()));
        baseMMSI += numAlliesIn;
        statements.addAll(buildStatements(numEnemiesOut, baseMMSI, enemies, currentDate, enemyOutStream.iterator()));
        
        final Collection<Statement> countryStatements =
                Sets.newHashSet(
                        vf.createStatement(vf.createURI(country + "UnitedStates"), countryFlag, vf.createLiteral("American")),
                        vf.createStatement(vf.createURI(country + "Russia"), countryFlag, vf.createLiteral("Russian")),
                        vf.createStatement(vf.createURI(country + "NorthKorea"), countryFlag, vf.createLiteral("North Korean")),
                        vf.createStatement(vf.createURI(country + "Iran"), countryFlag, vf.createLiteral("Iranian")),
                        vf.createStatement(vf.createURI(country + "Australia"), countryFlag, vf.createLiteral("Australian")),
                        vf.createStatement(vf.createURI(country + "Britian"), countryFlag, vf.createLiteral("British")),
                        
                        vf.createStatement(vf.createURI(country + "UnitedStates"), countryEnemy, vf.createURI(country + "Russia")),
                        vf.createStatement(vf.createURI(country + "UnitedStates"), countryEnemy, vf.createURI(country + "NorthKorea")),
                        vf.createStatement(vf.createURI(country + "UnitedStates"), countryEnemy, vf.createURI(country + "Iran")));
                
        statements.addAll(countryStatements);
        
        return statements;
        
    }
    
    private Set<Statement> buildStatements(int num, int mmsi, List<String> flags, long date, Iterator<Double> doubles) {
        Set<Statement> statements = new HashSet<>();
        Random random = new Random();
        for(int i = 0; i < num; i++) {
            double lat = -76 - doubles.next();
            double lon = 38 + doubles.next();
            Date randDate = new Date(date - random.nextInt(86400000));
            String randFlag = flags.get(random.nextInt(flags.size()));
            statements.addAll(buildStatements(mmsi++, randFlag, randDate, lat, lon));
        }
        return statements;
    }
    
    private Set<Statement> buildStatements(int mmsi, String flag, Date date, double lat, double lon) {
        
        Set<Statement> statements = new HashSet<>();
        URI vesselSubjURI = vf.createURI(vessel + mmsi);
        URI obsSubjURI = vf.createURI(obs + mmsi);
        statements.add(vf.createStatement(vesselSubjURI, vesselMmsi, vf.createLiteral(mmsi)));
        statements.add(vf.createStatement(vesselSubjURI, vesselFlag, vf.createLiteral(flag)));
        statements.add(vf.createStatement(obsSubjURI, obsMmsi, vf.createLiteral(mmsi)));
        statements.add(vf.createStatement(obsSubjURI, obsTime, vf.createLiteral(date)));
        String point = "Point(" + lat  + " " + lon + ")";
        statements.add(vf.createStatement(obsSubjURI, obsWKT, new LiteralImpl(point, wktLiteral)));
        
        return statements;
    }
        
    private void printResults(Set<?> results, String pcjId) {
        System.out.println("Retrieved the following results from Kafka topic: " + pcjId);
        System.out.println("");
        System.out.println("==================== QUERY RESULTS ==========================");
        results.forEach(x -> System.out.println(x));
        System.out.println("==================== END QUERY RESULTS ======================");
        System.out.println("");
        System.out.println("");
    }

    private String createConstructPcj(String sparql) throws MalformedQueryException, PcjException {
        prettyPrintQuery(sparql);
        CreatePcj createPcj = new CreatePcj();
        try (FluoClient client = new FluoClientImpl(super.getFluoConfiguration())) {
            FluoQuery query = createPcj.createFluoPcj(client, sparql);
            return query.getConstructQueryMetadata().get().getNodeId();
        }
    }

    private String createPcj(String sparql) throws InstanceDoesNotExistException, RyaClientException {
        // Register the PCJ with Rya.
        prettyPrintQuery(sparql);
        final Instance accInstance = getAccumuloConnector().getInstance();
        final Connector accumuloConn = getAccumuloConnector();

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(new AccumuloConnectionDetails(ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(), accInstance.getInstanceName(), accInstance.getZooKeepers()), accumuloConn);

        final String pcjId = ryaClient.getCreatePCJ().createPCJ(RYA_INSTANCE_NAME, sparql);
        return pcjId;
    }

    private void prettyPrintQuery(String query) {

        System.out.println("Registering the following query in Fluo: ");
        System.out.println("");
        for (String str : query.split("\\r?\\n")) {
            System.out.println(str);
        }
        System.out.println("");

    }

    private void loadData(final Collection<Statement> statements) throws Exception {
        System.out.println("");
        System.out.println("Loading Statements into Rya...");
        System.out.println("");
        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = super.getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();
    }

    private Set<VisibilityBindingSet> readAllResultsFromKafka(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read all of the results from the Kafka topic.
        final Set<VisibilityBindingSet> results = new HashSet<>();

        try (final KafkaConsumer<Integer, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<Integer, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<Integer, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                results.add(recordIterator.next().value());
            }
        }

        return results;
    }
    
    private Set<RyaSubGraph> readAllConstructResultsFromKafka(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        
        // Read all of the results from the Kafka topic.
        final Set<RyaSubGraph> results = new HashSet<>();

        try (final KafkaConsumer<String, RyaSubGraph> consumer = makeSubGraphConsumer(pcjId)) {
            final ConsumerRecords<String, RyaSubGraph> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, RyaSubGraph>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                results.add(recordIterator.next().value());
            }
        }

        return results;
    }
    
    
    protected KafkaConsumer<String, RyaSubGraph> makeSubGraphConsumer(final String TopicName) {
        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                RyaSubGraphKafkaSerDe.class.getName());

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, RyaSubGraph> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TopicName));
        return consumer;
    }
    

    private Set<VisibilityBindingSet> readGroupedResultsFromKafka(final String pcjId, final VariableOrder groupByVars) {
        requireNonNull(pcjId);

        // Read the results from the Kafka topic. The last one for each set of
        // Group By values is an aggregation result.
        // The key in this map is a Binding Set containing only the group by
        // variables.
        final Map<BindingSet, VisibilityBindingSet> results = new HashMap<>();

        try (final KafkaConsumer<Integer, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<Integer, VisibilityBindingSet> records = consumer.poll(15000);
            final Iterator<ConsumerRecord<Integer, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                final VisibilityBindingSet visBindingSet = recordIterator.next().value();

                final MapBindingSet key = new MapBindingSet();
                for (final String groupByBar : groupByVars) {
                    key.addBinding(visBindingSet.getBinding(groupByBar));
                }

                results.put(key, visBindingSet);
            }
        }
        return Sets.newHashSet(results.values());
    }
}
