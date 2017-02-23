package notification;

import java.util.Date;

public class TimestampedNotification extends BasicNotification {

    private Date date;

    public TimestampedNotification(String id) {
        super(id);
        date = new Date();
    }

    public TimestampedNotification(String id, String message) {
        super(id, message);
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
