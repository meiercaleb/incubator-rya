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

import notification.BasicNotification;

public class BasicNotificationTypeAdapter implements JsonDeserializer<BasicNotification>, JsonSerializer<BasicNotification> {

    @Override
    public JsonElement serialize(BasicNotification arg0, Type arg1, JsonSerializationContext arg2) {
        JsonObject result = new JsonObject();
        result.add("id", new JsonPrimitive(arg0.getId()));
        return result;
    }

    @Override
    public BasicNotification deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2) throws JsonParseException {
        JsonObject json = arg0.getAsJsonObject();
        String id = json.get("id").getAsString();
        return new BasicNotification(id);
    }

}
