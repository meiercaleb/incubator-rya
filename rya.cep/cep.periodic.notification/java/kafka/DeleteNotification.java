package kafka;

public class DeleteNotification extends CommandNotification {

	public DeleteNotification(String id) {
		super(Command.DELETE, new BasicNotification(id));
	}
}
