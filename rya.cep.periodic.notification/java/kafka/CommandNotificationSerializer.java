package kafka;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import api.Notification;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class CommandNotificationSerializer implements Encoder<CommandNotification>, Decoder<CommandNotification> {

	private static Gson gson = new GsonBuilder()
			.registerTypeHierarchyAdapter(Notification.class, new CommandNotificationTypeAdapter()).create();
	private static final Logger LOG = LoggerFactory.getLogger(CommandNotificationSerializer.class);
	private VerifiableProperties props;

	public CommandNotificationSerializer() {
	};

	// this constructor is needed for use with Kafka producers
	public CommandNotificationSerializer(VerifiableProperties props) {
		this.props = props;
	}

	@Override
	public byte[] toBytes(CommandNotification arg0) {
		try {
			return gson.toJson(arg0).getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.info("Unable to serialize CommandNotification: " + arg0);
			throw new RuntimeException(e);
		}
	}

	@Override
	public CommandNotification fromBytes(byte[] arg0) {
		try {
			String json = new String(arg0, "UTF-8");
			return gson.fromJson(json, CommandNotification.class);
		} catch (UnsupportedEncodingException e) {
			LOG.info("Invalid String encoding.");
			throw new RuntimeException(e);
		}
	}

}
