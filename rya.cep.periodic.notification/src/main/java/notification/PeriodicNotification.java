package notification;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import api.Notification;

public class PeriodicNotification implements Notification {

    private String id;
    private long period;
    private TimeUnit periodTimeUnit;
    private long initialDelay;
    private long startTime;

    public PeriodicNotification(String id, long startTime, long period, TimeUnit periodTimeUnit, long initialDelay) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(periodTimeUnit);
        Preconditions.checkArgument(startTime > 0 && period > 0 && initialDelay > 0);
        this.id = id;
        this.period = period;
        this.startTime = startTime;
        this.periodTimeUnit = periodTimeUnit;
        this.initialDelay = initialDelay;
    }
    

    public PeriodicNotification(PeriodicNotification other) {
        this(other.id, other.startTime, other.period, other.periodTimeUnit, other.initialDelay);
    }

    public String getId() {
        return id;
    }

    public long getPeriod() {
        return period;
    }

    public long getStartTime() {
        return startTime;
    }

    public TimeUnit getTimeUnit() {
        return periodTimeUnit;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String delim = "=";
        String delim2 = ";";
        return builder.append("id").append(delim).append(id).append(delim2).append("period").append(delim).append(period).append(delim2)
                .append("startTime").append(delim).append(startTime).append(delim2).append("periodTimeUnit").append(delim)
                .append(periodTimeUnit).append(delim2).append("initialDelay").append(delim).append(initialDelay).toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof PeriodicNotification)) {
            return false;
        }

        PeriodicNotification notification = (PeriodicNotification) other;
        return Objects.equals(this.id, notification.id) && (this.period == notification.period) && (this.startTime == notification.startTime)
                && Objects.equals(this.periodTimeUnit, notification.periodTimeUnit) && (this.initialDelay == notification.initialDelay);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + id.hashCode();
        result = 31 * result + Long.hashCode(period);
        result = 31 * result + Long.hashCode(startTime);
        result = 31 * result + this.periodTimeUnit.hashCode();
        result = 31 * result + Long.hashCode(initialDelay);
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private long period;
        private long startTime;
        private TimeUnit periodTimeUnit;
        private long initialDelay = 0;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder period(long period) {
            this.period = period;
            return this;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.periodTimeUnit = timeUnit;
            return this;
        }

        public Builder startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder initialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        public PeriodicNotification build() {
            return new PeriodicNotification(id, startTime, period, periodTimeUnit, initialDelay);
        }

    }

}
