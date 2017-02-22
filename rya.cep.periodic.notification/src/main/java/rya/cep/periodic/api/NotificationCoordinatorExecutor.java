package rya.cep.periodic.api;

import cep.periodic.notification.notification.CommandNotification;

public interface NotificationCoordinatorExecutor extends LifeCycle {

    public void processNextCommandNotification(CommandNotification notification);

}
