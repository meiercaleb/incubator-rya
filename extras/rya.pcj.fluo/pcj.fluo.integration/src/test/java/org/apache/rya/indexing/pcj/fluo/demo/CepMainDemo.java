package org.apache.rya.indexing.pcj.fluo.demo;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.ZkClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.google.common.collect.Sets;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class CepMainDemo {
    public static final String ACCUMULO_USER = "root";
    public static final String ACCUMULO_PASSWORD = "secret";

    private static File baseDir;
    private static MiniAccumuloCluster cluster;
    private FluoConfiguration fluoConfig;
    private MiniFluo miniFluo;
    protected static AtomicInteger tableCounter = new AtomicInteger(1);

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;

    // The Rya instance statements are written to that will be fed into the Fluo
    // app.
    protected static final String RYA_INSTANCE_NAME = "test_";
    private RyaSailRepository ryaSailRepo = null;
    private static ValueFactory vf = new ValueFactoryImpl();

    // *******************************************************************************
    // Mini Accumulo initialization and tear down
    // *******************************************************************************
    private static void setupMiniAccumulo() throws Exception {
        try {
            // try to put in target dir
            File targetDir = new File("target");
            if (targetDir.exists() && targetDir.isDirectory()) {
                baseDir = new File(targetDir, "accumuloExportIT-" + UUID.randomUUID());
            } else {
                baseDir = new File(FileUtils.getTempDirectory(), "accumuloExportIT-" + UUID.randomUUID());
            }

            FileUtils.deleteDirectory(baseDir);
            MiniAccumuloConfig cfg = new MiniAccumuloConfig(baseDir, ACCUMULO_PASSWORD);
            cluster = new MiniAccumuloCluster(cfg);
            cluster.start();
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void tearDownMiniAccumulo() {
        try {
        cluster.stop();
        FileUtils.deleteDirectory(baseDir);
        } catch(Exception e) {
            System.out.println("Encountered the following Exception when shutting down the MiniAccumulo cluster: " + e.getMessage());
        }
    }

    /**
     * Returns an Accumulo Connector to MiniAccumuloCluster
     */
    private Connector getAccumuloConnector() {
        try {
            return cluster.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    // *******************************************************************************
    // Fluo initialization and tear down
    // *******************************************************************************
    private void setupMiniFluo(boolean startMiniFluo) throws Exception {
        resetFluoConfig();
        preFluoInitHook();
        FluoFactory.newAdmin(fluoConfig).initialize(new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true));
        if (startMiniFluo) {
            miniFluo = FluoFactory.newMiniFluo(fluoConfig);
        } else {
            miniFluo = null;
        }
    }

    private void tearDownMiniFluo(){
        if (miniFluo != null) {
            miniFluo.close();
            miniFluo = null;
        }
    }

    private static void configureFromMAC(FluoConfiguration fluoConfig, MiniAccumuloCluster cluster) {
        fluoConfig.setMiniStartAccumulo(false);
        fluoConfig.setAccumuloInstance(cluster.getInstanceName());
        fluoConfig.setAccumuloUser("root");
        fluoConfig.setAccumuloPassword(cluster.getConfig().getRootPassword());
        fluoConfig.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
        fluoConfig.setAccumuloZookeepers(cluster.getZooKeepers());
    }

    private void resetFluoConfig() {
        fluoConfig = new FluoConfiguration();
        configureFromMAC(fluoConfig, cluster);
        fluoConfig.setApplicationName("fluo-it");
        fluoConfig.setAccumuloTable("fluo" + tableCounter.getAndIncrement());
    }

    private void preFluoInitHook() throws Exception {
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
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer");
        kafkaParams.addAllProducerConfig(producerConfig);

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(QueryResultObserver.class.getName(), exportParams);
        observers.add(exportObserverConfig);

        // create construct query observer and tell it not to export to Kafka
        // it will only add results back into Fluo
        HashMap<String, String> constructParams = new HashMap<>();
        final KafkaExportParameters kafkaConstructParams = new KafkaExportParameters(constructParams);
        kafkaConstructParams.setExportToKafka(false);

        final ObserverSpecification constructExportObserverConfig = new ObserverSpecification(ConstructQueryResultObserver.class.getName(),
                constructParams);
        observers.add(constructExportObserverConfig);

        // Add the observers to the Fluo Configuration.
        fluoConfig.addObservers(observers);
    }

    // *******************************************************************************
    // Kafka initialization and tear down
    // *******************************************************************************
    private void setupKafka() throws Exception {
        // Setup Kafka.
        zkServer = new EmbeddedZookeeper();
        final String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils.apply(zkClient, false);

        // setup Broker
        final Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        final KafkaConfig config = new KafkaConfig(brokerProps);
        final Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    private void teardownKafka() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    // *******************************************************************************
    // Rya initialization and tear down
    // *******************************************************************************
    private void installRyaInstance() throws Exception {
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        // Install the Rya instance to the mini accumulo cluster.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ACCUMULO_USER, ACCUMULO_PASSWORD.toCharArray(), instanceName, zookeepers),
                getAccumuloConnector());

        ryaClient.getInstall().install(RYA_INSTANCE_NAME,
                InstallConfiguration.builder().setEnableTableHashPrefix(false).setEnableFreeTextIndex(false)
                        .setEnableEntityCentricIndex(false).setEnableGeoIndex(false).setEnableTemporalIndex(false).setEnablePcjIndex(true)
                        .setFluoPcjAppName(fluoConfig.getApplicationName()).build());

        // Connect to the Rya instance that was just installed.
        final AccumuloRdfConfiguration conf = makeConfig(instanceName, zookeepers);
        final Sail sail = RyaSailFactory.getInstance(conf);
        ryaSailRepo = new RyaSailRepository(sail);
    }

    private void teardownRya() {
        final MiniAccumuloCluster cluster = CepMainDemo.cluster;
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        // Uninstall the instance of Rya.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ACCUMULO_USER, ACCUMULO_PASSWORD.toCharArray(), instanceName, zookeepers),
                getAccumuloConnector());

        try {
            ryaClient.getUninstall().uninstall(RYA_INSTANCE_NAME);
            ryaSailRepo.shutDown();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception while closing Rya: " + e.getMessage());
        }
    }

    private AccumuloRdfConfiguration makeConfig(final String instanceName, final String zookeepers) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(RYA_INSTANCE_NAME);

        // Accumulo connection information.
        conf.setAccumuloUser(AccumuloExportITBase.ACCUMULO_USER);
        conf.setAccumuloPassword(AccumuloExportITBase.ACCUMULO_PASSWORD);
        conf.setAccumuloInstance(getAccumuloConnector().getInstance().getInstanceName());
        conf.setAccumuloZookeepers(getAccumuloConnector().getInstance().getZooKeepers());
        conf.setAuths("");

        // PCJ configuration information.
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_PCJ_UPDATER_INDEX, "true");
        conf.set(ConfigUtils.FLUO_APP_NAME, fluoConfig.getApplicationName());
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());

        conf.setDisplayQueryPlan(true);

        return conf;
    }

    private void init() throws Exception {
        setupMiniAccumulo();
        setupKafka();
    }

    private void tearDown() {
        tearDownMiniFluo();
        teardownKafka();
        tearDownMiniAccumulo();
    }

    private void initExample() throws Exception {
        setupMiniFluo(true);
        installRyaInstance();
    }

    private void tearDownExample() {
        tearDownMiniFluo();
        teardownRya();
    }

    /****************************************************************************
     * Main Method
     ****************************************************************************/
    public static void main(String[] args) {

        CepMainDemo demo = new CepMainDemo();
        try {
            demo.init();
            // demo.filterExample();
            // demo.aggregationExample();
            demo.constructQueryExample();
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down the application: " + e.getMessage());
        } finally {
            demo.tearDown();
        }
    }

    private void filterExample() {
        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING FILTER EXAMPLE");
        System.out.println("**************************************************************************");
        System.out.println("");

    }

    private void aggregationExample() {
        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING AGGREGATION EXAMPLE");
        System.out.println("**************************************************************************");
        System.out.println("");
    }

    private void constructQueryExample() {
        // A query that groups what is aggregated by one of the keys.
        final String construct = "CONSTRUCT { ?x <urn:type> <urn:grandMother> .\n" + "?z <urn:grandDaughterOf> ?x } \n" + "WHERE { \n"
                + "?x <urn:motherOf> ?y. \n " + "?y <urn:motherOf> ?z. \n" + "?z <urn:type> <urn:female> }";

        final String aggregation = "SELECT ?x (count(?y) as ?totalGrandDaughters) { \n" + "?x <urn:type> <urn:grandMother> . \n"
                + "?y <urn:grandDaughterOf> ?x } \n" + "GROUP BY ?x";

        final Set<Statement> statements = Sets.newHashSet(
                // Betty's granchildren
                vf.createStatement(vf.createURI("urn:Betty"), vf.createURI("urn:motherOf"), vf.createURI("urn:Suzie")),
                vf.createStatement(vf.createURI("urn:Suzie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Joan")),
                vf.createStatement(vf.createURI("urn:Joan"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Suzie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Sara")),
                vf.createStatement(vf.createURI("urn:Sara"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Suzie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Evan")),
                vf.createStatement(vf.createURI("urn:Evan"), vf.createURI("urn:type"), vf.createURI("urn:male")),
                // Jennifer's grandchildren
                vf.createStatement(vf.createURI("urn:Jennifer"), vf.createURI("urn:motherOf"), vf.createURI("urn:Debbie")),
                vf.createStatement(vf.createURI("urn:Debbie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Carol")),
                vf.createStatement(vf.createURI("urn:Carol"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Debbie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Emily")),
                vf.createStatement(vf.createURI("urn:Emily"), vf.createURI("urn:type"), vf.createURI("urn:female")),
                vf.createStatement(vf.createURI("urn:Debbie"), vf.createURI("urn:motherOf"), vf.createURI("urn:Isabelle")),
                vf.createStatement(vf.createURI("urn:Isabelle"), vf.createURI("urn:type"), vf.createURI("urn:female")));

        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING CONSTRUCT QUERY EXAMPLE");
        System.out.println("**************************************************************************");
        System.out.println("");
        exampleBase(Optional.of(construct), aggregation, statements, Optional.of(new VariableOrder("x")));
    }

    private void exampleBase(Optional<String> constructQuery, String sparql, Set<Statement> statements,
            Optional<VariableOrder> groupedVarOrder) {
        try {
            initExample();
            if (constructQuery.isPresent()) {
                createConstructPcj(constructQuery.get());
            }
            String pcjId = createPcj(sparql);

            loadData(statements);
            miniFluo.waitForObservers();

            Set<VisibilityBindingSet> results = null;
            if (groupedVarOrder.isPresent()) {
                results = readGroupedResults(pcjId, groupedVarOrder.get());
            } else {
                results = readAllResults(pcjId);
            }

            System.out.println("Retrieved the following results from Kafka: ");
            System.out.println("");
            System.out.println("==================== QUERY RESULTS ==========================");
            results.forEach(x -> System.out.println(x));
            System.out.println("==================== END QUERY RESULTS ======================");
            System.out.println("");
            System.out.println("");
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when running an example: " + e.getMessage());
        } finally {
            tearDownExample();
        }
    }

    // ****************************************************************************
    // Methods for loading and retrieving data
    // ****************************************************************************
    private KafkaConsumer<Integer, VisibilityBindingSet> makeConsumer(final String TopicName) {
        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer");

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<Integer, VisibilityBindingSet> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TopicName));
        return consumer;
    }

    private void createConstructPcj(String sparql) throws MalformedQueryException, PcjException {
        prettyPrintQuery(sparql);
        CreatePcj createPcj = new CreatePcj();
        FluoClient client = new FluoClientImpl(fluoConfig);
        createPcj.createFluoPcj(client, sparql);
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

        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = ryaSailRepo.getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        miniFluo.waitForObservers();
    }

    private Set<VisibilityBindingSet> readAllResults(final String pcjId) throws Exception {
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

    private Set<VisibilityBindingSet> readGroupedResults(final String pcjId, final VariableOrder groupByVars) {
        requireNonNull(pcjId);

        // Read the results from the Kafka topic. The last one for each set of
        // Group By values is an aggregation result.
        // The key in this map is a Binding Set containing only the group by
        // variables.
        final Map<BindingSet, VisibilityBindingSet> results = new HashMap<>();

        try (final KafkaConsumer<Integer, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<Integer, VisibilityBindingSet> records = consumer.poll(5000);
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
