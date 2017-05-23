package org.apache.rya.cep.periodic.api;

import java.util.concurrent.TimeUnit;

import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;

public interface PeriodicNotificationClient {

    public void addNotification(PeriodicNotification notification);
    
    public void deleteNotification(BasicNotification notification);
    
    public void deleteNotification(String notificationId);
    
    public void addNotification(String id, long period, long delay, TimeUnit unit);
    
    public void close();
    
}
