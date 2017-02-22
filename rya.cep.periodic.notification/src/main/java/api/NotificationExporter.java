package api;

public interface NotificationExporter extends LifeCycle {

	public void exportNotification(Notification notification);
	
}
