package org.apache.rya.periodic.notification.notification;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TimestampedNotification extends PeriodicNotification {

    private Date date;

    public TimestampedNotification(String id, long period, TimeUnit periodTimeUnit, long initialDelay) {
        super(id, period, periodTimeUnit, initialDelay);
        date = new Date();
    }
    
    public TimestampedNotification(PeriodicNotification notification) {
        super(notification);
        date = new Date();
    }

    public Date getTimestamp() {
        return date;
    }

    @Override
    public String toString() {
        return super.toString() + ";date=" + date;
    }

}
