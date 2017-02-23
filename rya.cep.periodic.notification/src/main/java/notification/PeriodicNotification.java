package notification;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import api.Notification;

public class PeriodicNotification implements Notification {

    private String id;
    private long period;
    private TimeUnit periodTimeUnit;
    private TimeUnit initialDelayTimeUnit;
    private long initialDelay;
    private Optional<String> message;

    private PeriodicNotification(String id, long period, TimeUnit periodTimeUnit, long initialDelay,
            TimeUnit initialDelayTimeUnit, String message) {
        this.id = id;
        this.period = period;
        this.periodTimeUnit = periodTimeUnit;
        this.initialDelayTimeUnit = initialDelayTimeUnit;
        this.initialDelay = initialDelay;
        this.message = Optional.ofNullable(message);
    }

    public String getId() {
        return id;
    }

    public long getPeriod() {
        return period;
    }

    public TimeUnit getPeriodTimeUnit() {
        return periodTimeUnit;
    }

    public TimeUnit getInitialDelayTimeUnit() {
        return initialDelayTimeUnit;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public Optional<String> getMessage() {
        return message;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String delim = "=";
        String delim2 = ";";
        return builder.append("id").append(delim).append(id).append(delim2).append("period").append(delim)
                .append(period).append(delim2).append("periodTimeUnit").append(delim).append(periodTimeUnit)
                .append(delim2).append("initialDelay").append(delim).append(initialDelay).append(delim2)
                .append("initialDelayTimeUnit").append(delim).append(initialDelayTimeUnit).append(delim2)
                .append("message").append(delim).append(message.toString()).toString();
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
        return Objects.equals(this.id, notification.id) && (this.period == notification.period)
                && Objects.equals(this.periodTimeUnit, notification.periodTimeUnit)
                && (this.initialDelay == notification.initialDelay)
                && Objects.equals(this.initialDelayTimeUnit, notification.initialDelayTimeUnit)
                && Objects.equals(this.message, notification.message);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + id.hashCode();
        result = 31 * result + Long.hashCode(period);
        result = 31 * result + this.periodTimeUnit.hashCode();
        result = 31 * result + Long.hashCode(initialDelay);
        result = 31 * result + this.initialDelayTimeUnit.hashCode();
        result = 31 * result + Objects.hash(message);
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private long period;
        private TimeUnit periodTimeUnit;
        private TimeUnit initialDelayTimeUnit;
        private long initialDelay = 0;
        private String message = null;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder period(long period) {
            this.period = period;
            return this;
        }

        public Builder periodTimeUnit(TimeUnit timeUnit) {
            this.periodTimeUnit = timeUnit;
            return this;
        }

        public Builder initialDelayTimeUnit(TimeUnit timeUnit) {
            this.initialDelayTimeUnit = timeUnit;
            return this;
        }

        public Builder initialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public PeriodicNotification build() {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(period > 0);
            Preconditions.checkNotNull(periodTimeUnit);
            if (initialDelayTimeUnit == null) {
                if (initialDelay > 0) {
                    throw new IllegalStateException("Must specify TimeUnit for initial delay.");
                } else {
                    initialDelayTimeUnit = periodTimeUnit;
                }
            }
            return new PeriodicNotification(id, period, periodTimeUnit, initialDelay, initialDelayTimeUnit, message);
        }

    }

}
