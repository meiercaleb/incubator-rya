package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import api.LifeCycle;
import api.PeriodicNotificationCoordinator;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaNotificationProvider implements LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationProvider.class);
	private ConsumerConnector consumer;
	private String topic;
	private ExecutorService executor;
	private Properties props;
	private PeriodicNotificationCoordinator coord;

	public KafkaNotificationProvider(Properties props, PeriodicNotificationCoordinator coord) {
		this.props = props;
		this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(props));
		this.topic = props.getProperty("kafka.topic");
		this.coord = coord;
	}

	@Override
	public void stop() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				LOG.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			LOG.info("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void start() {
		int threads = Integer.parseInt(props.getProperty("kafka.consumer.threads", "1"));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		LOG.info("Creating topic map with " + threads + " threads.");
		topicCountMap.put(topic, threads);
		Map<String, List<KafkaStream<String, CommandNotification>>> consumerMap = consumer.createMessageStreams(
				topicCountMap, new StringDecoder(new VerifiableProperties()), new CommandNotificationSerializer());
		List<KafkaStream<String, CommandNotification>> streams = consumerMap.get(topic);

		// now launch all the threads
		//
		executor = Executors.newFixedThreadPool(threads);

		// now create an object to consume the messages
		//
		int threadNumber = 0;
		for (final KafkaStream<String, CommandNotification> stream : streams) {
			LOG.info("Creating consumer:" + threadNumber);
			executor.submit(new PeriodicNotificationConsumer(stream, threadNumber, coord));
			threadNumber++;
		}
	}

	private static ConsumerConfig createConsumerConfig(Properties props) {
		return new ConsumerConfig(props);
	}


}
