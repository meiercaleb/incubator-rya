package org.apache.rya.periodic.notification.notification;

import org.apache.rya.cep.periodic.api.Notification;

import com.google.common.base.Objects;

public class BasicNotification implements Notification {

    private String id;

    public BasicNotification(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof BasicNotification) {
            BasicNotification not = (BasicNotification) other;
            return Objects.equal(this.id, not.id);
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result * 31 + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        return builder.append("id").append("=").append(id).toString();
    }

}
