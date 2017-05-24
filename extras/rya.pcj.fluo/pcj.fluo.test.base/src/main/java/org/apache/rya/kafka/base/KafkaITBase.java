package org.apache.rya.kafka.base;

import java.nio.file.Files;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class KafkaITBase {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    
    @Before
    public void setupKafka() throws Exception {

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
    
    /**
     * Close all the Kafka mini server and mini-zookeeper
     *
     * @see org.apache.rya.indexing.pcj.fluo.ITBase#shutdownMiniResources()
     */
    @After
    public void teardownKafka() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
    
}
