package api;

import notification.TimestampedNotification;

public interface NotificationProcessor {

    public void processNotification(TimestampedNotification notification);
    
}
