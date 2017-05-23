package api;

import kafka.CommandNotification;
import kafka.TimestampedNotification;

public interface PeriodicNotificationCoordinator extends LifeCycle{

	public void processNextCommandNotification(CommandNotification notification);
	
	public TimestampedNotification getNextPeriodicNotification();
}
