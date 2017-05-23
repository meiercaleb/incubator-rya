package org.apache.rya.cep.periodic.api;

import org.apache.rya.periodic.notification.notification.CommandNotification;

public interface NotificationCoordinatorExecutor extends LifeCycle {

    public void processNextCommandNotification(CommandNotification notification);

}
