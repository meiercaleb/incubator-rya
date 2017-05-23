package kafka;

import java.util.Optional;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import api.Notification;

public class BasicNotification implements Notification {

	private String id;
	private Optional<String> message;

	public BasicNotification(String id) {
		this(id, null);
	}

	public BasicNotification(String id, String message) {
		Preconditions.checkNotNull(id);
		this.id = id;
		this.message = Optional.ofNullable(message);
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public Optional<String> getMessage() {
		return message;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}

		if (other instanceof BasicNotification) {
			BasicNotification not = (BasicNotification) other;
			return Objects.equal(this.id, not.id) && Objects.equal(this.message, not.message);
		}

		return false;
	}

	@Override
	public int hashCode() {
		int result = 17;
		result = result * 31 + id.hashCode();
		result = result * 31 + Objects.hashCode(message);
		return result;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		return builder.append("id").append("=").append(id).append(";").append("message").append("=").append(message)
				.toString();
	}

}
