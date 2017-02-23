package notification;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import notification.PeriodicNotification.Builder;

public class PeriodicNotificationTypeAdapter
        implements JsonSerializer<PeriodicNotification>, JsonDeserializer<PeriodicNotification> {

    @Override
    public PeriodicNotification deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2)
            throws JsonParseException {

        JsonObject json = arg0.getAsJsonObject();
        String id = json.get("id").getAsString();
        long period = json.get("period").getAsLong();
        TimeUnit periodTimeUnit = TimeUnit.valueOf(json.get("periodTimeUnit").getAsString());
        long initialDelay = json.get("initialDelay").getAsLong();
        TimeUnit initialDelayTimeUnit = TimeUnit.valueOf(json.get("initialDelayTimeUnit").getAsString());
        Builder builder = PeriodicNotification.builder().id(id).period(period).periodTimeUnit(periodTimeUnit)
                .initialDelay(initialDelay).initialDelayTimeUnit(initialDelayTimeUnit);

        JsonElement element = json.get("message");
        if (element != null) {
            builder.message(element.getAsString());
        }

        return builder.build();
    }

    @Override
    public JsonElement serialize(PeriodicNotification arg0, Type arg1, JsonSerializationContext arg2) {

        JsonObject result = new JsonObject();
        result.add("id", new JsonPrimitive(arg0.getId()));
        result.add("period", new JsonPrimitive(arg0.getPeriod()));
        result.add("periodTimeUnit", new JsonPrimitive(arg0.getPeriodTimeUnit().name()));
        result.add("initialDelay", new JsonPrimitive(arg0.getInitialDelay()));
        result.add("initialDelayTimeUnit", new JsonPrimitive(arg0.getInitialDelayTimeUnit().name()));
        Optional<String> message = arg0.getMessage();
        if (message.isPresent()) {
            result.add("message", new JsonPrimitive(message.get()));
        }

        return result;
    }

}
