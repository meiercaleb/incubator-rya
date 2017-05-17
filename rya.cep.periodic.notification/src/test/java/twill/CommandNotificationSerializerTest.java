package twill;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import cep.periodic.notification.notification.BasicNotification;
import cep.periodic.notification.notification.CommandNotification;
import cep.periodic.notification.notification.PeriodicNotification;
import cep.periodic.notification.notification.CommandNotification.Command;
import cep.periodic.notification.serialization.CommandNotificationSerializer;

public class CommandNotificationSerializerTest {

    private CommandNotificationSerializer serializer = new CommandNotificationSerializer();

    @Test
    public void basicSerializationTest() {
        PeriodicNotification notification = PeriodicNotification.builder().id(UUID.randomUUID().toString()).period(24)
                .timeUnit(TimeUnit.DAYS).initialDelay(1).build();
        CommandNotification command = new CommandNotification(Command.ADD, notification);
        Assert.assertEquals(command, serializer.fromBytes(serializer.toBytes(command)));

        PeriodicNotification notification1 = PeriodicNotification.builder().id(UUID.randomUUID().toString()).period(32)
                .timeUnit(TimeUnit.SECONDS).initialDelay(15).build();
        CommandNotification command1 = new CommandNotification(Command.ADD, notification1);
        Assert.assertEquals(command1, serializer.fromBytes(serializer.toBytes(command1)));

        PeriodicNotification notification2 = PeriodicNotification.builder().id(UUID.randomUUID().toString()).period(32)
                .timeUnit(TimeUnit.SECONDS).initialDelay(15).build();
        CommandNotification command2 = new CommandNotification(Command.ADD, notification2);
        Assert.assertEquals(command2, serializer.fromBytes(serializer.toBytes(command2)));

        BasicNotification notification3 = new BasicNotification(UUID.randomUUID().toString());
        CommandNotification command3 = new CommandNotification(Command.ADD, notification3);
        Assert.assertEquals(command3, serializer.fromBytes(serializer.toBytes(command3)));

    }

}
