package kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.rya.cep.periodic.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationProvider;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationRegistrationClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

public class PeriodicCommandNotificationConsumerIT {

    private int brokerId = 0;
    private String topic = "notifications";
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private String zkConnect;
    private int port;
    private Properties props;
    private KafkaConfig config;
    private Time mock;
    private KafkaServer kafkaServer;
    private KafkaNotificationProvider pncg;
    private NotificationCoordinatorExecutor coord;

    @Before
    public void setUp() {
        zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        port = TestUtils.choosePort();
        props = TestUtils.createBrokerConfig(brokerId, port, true);
        config = new KafkaConfig(props);
        mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        coord = new PeriodicNotificationCoordinatorExecutor(2);
        coord.start();
    }

    @Test
    public void consumerTest() throws InterruptedException, IOException {

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.createTopic(zkClient, topic, 2, 1, scala.collection.JavaConversions.asScalaBuffer(servers),
                new Properties());
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0,
                5000);

        pncg = new KafkaNotificationProvider(createConsumerConfig(zkConnect), coord);
        pncg.start();
        
        // setup producer
        KafkaNotificationRegistrationClient client = new KafkaNotificationRegistrationClient(topic,
                TestUtils.getProducerConfig("localhost:" + port));

        //create message to produce
        String id = UUID.randomUUID().toString();
        PeriodicNotification notification = PeriodicNotification.builder().id(id).period(5)
                .periodTimeUnit(TimeUnit.SECONDS).message("Hello there!").build();
        //register periodic notification
        client.addNotification(notification);

        List<TimestampedNotification> list = new ArrayList<>();
        long now = System.currentTimeMillis();

        // retrieve periodic notifications
        while (System.currentTimeMillis() < now + 10000) {
            list.add(coord.getNextPeriodicNotification());
        }

        //un-register periodic notification
        client.deleteNotification(id);
        now = System.currentTimeMillis();

        // verify no more notifications produced
        while (System.currentTimeMillis() < now + 10000) {
            if (coord.hasNextNotification()) {
                list.add(coord.getNextPeriodicNotification());
            }
        }

        System.out.println("Finished consuming notifications: " + new Date());
        System.out.println("Consumed Notifications: " + list);
        Assert.assertEquals(3, list.size());
        client.close();

    }

    @After
    public void cleanUp() {
        pncg.stop();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    private Properties createConsumerConfig(String zkConnect) {
        Properties props = new Properties();
        props.put("kafka.topic", topic);
        props.put("kafka.thread.number", "2");
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", "group");
        props.put("zookeeper.session.timeout.ms", "6000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        return props;
    }
}