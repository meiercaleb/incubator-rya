package org.apache.rya.cep.periodic.api;

import org.apache.rya.periodic.notification.notification.TimestampedNotification;

public interface NotificationProcessor {

    public void processNotification(TimestampedNotification notification);
    
}
