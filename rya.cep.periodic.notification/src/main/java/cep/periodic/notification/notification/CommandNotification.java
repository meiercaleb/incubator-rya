package cep.periodic.notification.notification;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import rya.cep.periodic.api.Notification;

public class CommandNotification implements Notification {

    private Notification notification;
    private Command command;

    public enum Command {
        ADD, DELETE
    };

    public CommandNotification(Command command, Notification notification) {
        Preconditions.checkNotNull(notification);
        Preconditions.checkNotNull(command);
        this.command = command;
        this.notification = notification;
    }

    @Override
    public String getId() {
        return notification.getId();
    }

    public Notification getNotification() {
        return this.notification;
    }

    public Command getCommand() {
        return this.command;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof CommandNotification) {
            CommandNotification cn = (CommandNotification) other;
            return Objects.equal(this.command, cn.command) && Objects.equal(this.notification, cn.notification);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hashCode(command);
        result = 31 * result + Objects.hashCode(notification);
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("command").append("=").append(command.toString()).append(";")
                .append(notification.toString()).toString();
    }

}
