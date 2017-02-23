package api;

import java.util.concurrent.TimeUnit;

import notification.BasicNotification;
import notification.PeriodicNotification;

public interface PeriodicNotificationClient {

    public void addNotification(PeriodicNotification notification);
    
    public void deleteNotification(BasicNotification notification);
    
    public void deleteNotification(String notificationId);
    
    public void addNotification(String id, String message, long period, long delay, TimeUnit unit);
    
    public void close();
    
}
