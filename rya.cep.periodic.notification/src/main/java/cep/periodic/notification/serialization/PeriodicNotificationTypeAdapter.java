package cep.periodic.notification.serialization;

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import cep.periodic.notification.notification.PeriodicNotification;
import cep.periodic.notification.notification.PeriodicNotification.Builder;

public class PeriodicNotificationTypeAdapter
        implements JsonSerializer<PeriodicNotification>, JsonDeserializer<PeriodicNotification> {

    @Override
    public PeriodicNotification deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2)
            throws JsonParseException {

        JsonObject json = arg0.getAsJsonObject();
        String id = json.get("id").getAsString();
        long period = json.get("period").getAsLong();
        TimeUnit periodTimeUnit = TimeUnit.valueOf(json.get("timeUnit").getAsString());
        long initialDelay = json.get("initialDelay").getAsLong();
        Builder builder = PeriodicNotification.builder().id(id).period(period)
                .initialDelay(initialDelay).timeUnit(periodTimeUnit);

        return builder.build();
    }

    @Override
    public JsonElement serialize(PeriodicNotification arg0, Type arg1, JsonSerializationContext arg2) {

        JsonObject result = new JsonObject();
        result.add("id", new JsonPrimitive(arg0.getId()));
        result.add("period", new JsonPrimitive(arg0.getPeriod()));
        result.add("initialDelay", new JsonPrimitive(arg0.getInitialDelay()));
        result.add("timeUnit", new JsonPrimitive(arg0.getTimeUnit().name()));

        return result;
    }

}
