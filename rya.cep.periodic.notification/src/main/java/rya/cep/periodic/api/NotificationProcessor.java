package rya.cep.periodic.api;

import cep.periodic.notification.notification.TimestampedNotification;

public interface NotificationProcessor {

    public void processNotification(TimestampedNotification notification);
    
}
