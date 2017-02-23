package api;

import notification.CommandNotification;
import notification.TimestampedNotification;

public interface PeriodicNotificationCoordinator extends LifeCycle {

    public void processNextCommandNotification(CommandNotification notification);

    public TimestampedNotification getNextPeriodicNotification();
    
    public boolean hasNextNotification();
}
