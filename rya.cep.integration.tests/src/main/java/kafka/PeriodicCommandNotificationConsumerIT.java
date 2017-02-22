package kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import api.PeriodicNotificationCoordinator;
import coordinator.ScheduledExecutorServiceCoordinator;
import kafka.CommandNotification.Command;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
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
	private PeriodicNotificationCoordinator coord;

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

		coord = new ScheduledExecutorServiceCoordinator(2);
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

		// setup producer
		Properties properties = TestUtils.getProducerConfig("localhost:" + port);
		properties.setProperty("key.serializer.class", StringEncoder.class.getName());
		properties.setProperty("serializer.class", CommandNotificationSerializer.class.getName());
		ProducerConfig producerConfig = new ProducerConfig(properties);
		Producer<String, CommandNotification> producer = new Producer<String, CommandNotification>(producerConfig);

		String id = UUID.randomUUID().toString();
		PeriodicNotification notification = PeriodicNotification.builder().id(id).period(5)
				.periodTimeUnit(TimeUnit.SECONDS).message("Hello there!").build();
		CommandNotification command = new CommandNotification(Command.ADD, notification);

		System.out.println("Producing Notifications: " + new Date());

		// generate and produce messages
		producer.send(scala.collection.JavaConversions
				.asScalaBuffer(Arrays.asList(new KeyedMessage<String, CommandNotification>(topic, id, command))));
		producer.close();

		pncg = new KafkaNotificationProvider(createConsumerConfig(zkConnect), coord);
		pncg.start();

		List<TimestampedNotification> list = new ArrayList<>();

		long now = System.currentTimeMillis();

		while (System.currentTimeMillis() < now + 10000) {
			list.add(coord.getNextPeriodicNotification());
		}

		System.out.println("Finished consuming notifications: " + new Date());
		System.out.println("Consumed Notifications: " + list);

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