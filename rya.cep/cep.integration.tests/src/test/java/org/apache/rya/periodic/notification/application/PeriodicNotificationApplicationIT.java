package org.apache.rya.periodic.notification.application;

import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.I0Itec.zkclient.ZkClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.FluoITHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.cep.periodic.api.CreatePeriodicQuery;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoClientFactory;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationRegistrationClient;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;

import com.google.common.collect.Sets;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class PeriodicNotificationApplicationIT extends RyaExportITBase {

    private PeriodicNotificationApplication app;
    private KafkaNotificationRegistrationClient registrar;
    private KafkaProducer<String, CommandNotification> producer;
    private Properties props;
    PeriodicNotificationApplicationConfiguration conf;
    
    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    
    @Before
    public void init() throws Exception {
        setUpKafka();
        props = getProps();
        conf = new PeriodicNotificationApplicationConfiguration(props);
        app = PeriodicNotificationApplicationFactory.getPeriodicApplication(props);
        producer = new KafkaProducer<>(props, new StringSerializer(), new CommandNotificationSerializer());
        registrar = new KafkaNotificationRegistrationClient(conf.getNotificationTopic(), producer);
    }
    
    private void setUpKafka() throws Exception {
        // Setup Kafka.
        zkServer = new EmbeddedZookeeper();
        final String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

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
    
    @Test
    public void periodicApplicationTest() throws Exception {

        String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n

        Properties props = getProps();

        try (FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf)) {
            Connector connector = ConfigUtils.getConnector(conf);
            PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(connector, conf.getTablePrefix());
            CreatePeriodicQuery periodicQuery = new CreatePeriodicQuery(fluo, storage);
            String id = periodicQuery.createQueryAndRegisterWithKafka(sparql, registrar);
            addData();
            app.start();
//            
            try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new BindingSetSerDe())) {
                consumer.subscribe(Arrays.asList(id));
                long end = System.currentTimeMillis() + 61000;
                while (System.currentTimeMillis() < end) {
                    ConsumerRecords<String, BindingSet> records = consumer.poll(15000);
                    records.forEach(x -> System.out.println(x.value()));
                }
            }
            
            CloseableIterator<BindingSet> results = storage.listResults(id, Optional.empty());
            results.forEachRemaining(x -> System.out.println(x));
        }

    }
    
    @After
    public void shutdown() {
        registrar.close();
        app.stop();
        teardownKafka();
    }
    
    private void teardownKafka() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    private void addData() throws DatatypeConfigurationException {
        // create statements to ingest into Fluo
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusSeconds(15);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusSeconds(15);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusSeconds(15);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusSeconds(15);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasId"), vf.createLiteral("id_4")));

        // add statements to Fluo
        try (FluoClient fluo = new FluoClientImpl(getFluoConfiguration())) {
            InsertTriples inserter = new InsertTriples();
            statements.forEach(x -> inserter.insert(fluo, RdfToRyaConversions.convertStatement(x)));
            getMiniFluo().waitForObservers();
            FluoITHelper.printFluoTable(fluo);
        }

    }

    private Properties getProps() {
        Properties props = new Properties();
        FluoConfiguration fluoConf = getFluoConfiguration();
        props.setProperty("accumulo.user", ACCUMULO_USER);
        props.setProperty("accumulo.password", ACCUMULO_PASSWORD);
        props.setProperty("accumulo.instance", getMiniAccumuloCluster().getInstanceName());
        props.setProperty("accumulo.auths", "");
        props.setProperty("accumulo.zookeepers", getMiniAccumuloCluster().getZooKeepers());
        props.setProperty("accumulo.rya.prefix", RYA_INSTANCE_NAME);
        props.setProperty(PeriodicNotificationApplicationConfiguration.FLUO_APP_NAME, fluoConf.getApplicationName());
        props.setProperty(PeriodicNotificationApplicationConfiguration.FLUO_TABLE_NAME, fluoConf.getAccumuloTable());
        props.setProperty(PeriodicNotificationApplicationConfiguration.KAFKA_BOOTSTRAP_SERVERS, BROKERHOST + ":" + BROKERPORT);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

}
