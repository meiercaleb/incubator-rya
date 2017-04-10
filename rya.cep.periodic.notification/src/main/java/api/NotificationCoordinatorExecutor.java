package api;

import notification.CommandNotification;

public interface NotificationCoordinatorExecutor extends LifeCycle {

    public void processNextCommandNotification(CommandNotification notification);

}
