package rya.cep.periodic.api;

import java.util.concurrent.TimeUnit;

import cep.periodic.notification.notification.BasicNotification;
import cep.periodic.notification.notification.PeriodicNotification;

public interface PeriodicNotificationClient {

    public void addNotification(PeriodicNotification notification);
    
    public void deleteNotification(BasicNotification notification);
    
    public void deleteNotification(String notificationId);
    
    public void addNotification(String id, long startTime, long period, long delay, TimeUnit unit);
    
    public void close();
    
}
