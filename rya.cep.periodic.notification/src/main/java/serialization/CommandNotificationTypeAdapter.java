package serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import api.Notification;
import notification.BasicNotification;
import notification.CommandNotification;
import notification.PeriodicNotification;
import notification.CommandNotification.Command;

public class CommandNotificationTypeAdapter
        implements JsonDeserializer<CommandNotification>, JsonSerializer<CommandNotification> {

    @Override
    public JsonElement serialize(CommandNotification arg0, Type arg1, JsonSerializationContext arg2) {
        JsonObject result = new JsonObject();
        result.add("command", new JsonPrimitive(arg0.getCommand().name()));
        Notification notification = arg0.getNotification();
        if (notification instanceof PeriodicNotification) {
            result.add("type", new JsonPrimitive(PeriodicNotification.class.getSimpleName()));
            PeriodicNotificationTypeAdapter adapter = new PeriodicNotificationTypeAdapter();
            result.add("notification",
                    adapter.serialize((PeriodicNotification) notification, PeriodicNotification.class, arg2));
        } else if (notification instanceof BasicNotification) {
            result.add("type", new JsonPrimitive(BasicNotification.class.getSimpleName()));
            BasicNotificationTypeAdapter adapter = new BasicNotificationTypeAdapter();
            result.add("notification",
                    adapter.serialize((BasicNotification) notification, BasicNotification.class, arg2));
        } else {
            throw new IllegalArgumentException("Invalid notification type.");
        }
        return result;
    }

    @Override
    public CommandNotification deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2)
            throws JsonParseException {

        JsonObject json = arg0.getAsJsonObject();
        Command command = Command.valueOf(json.get("command").getAsString());
        String type = json.get("type").getAsString();
        Notification notification = null;
        if (type.equals(PeriodicNotification.class.getSimpleName())) {
            notification = (new PeriodicNotificationTypeAdapter()).deserialize(json.get("notification"),
                    PeriodicNotification.class, arg2);
        } else if (type.equals(BasicNotification.class.getSimpleName())) {
            notification = (new BasicNotificationTypeAdapter()).deserialize(json.get("notification"),
                    BasicNotification.class, arg2);
        } else {
            throw new JsonParseException("Cannot deserialize Json");
        }

        return new CommandNotification(command, notification);
    }

}
