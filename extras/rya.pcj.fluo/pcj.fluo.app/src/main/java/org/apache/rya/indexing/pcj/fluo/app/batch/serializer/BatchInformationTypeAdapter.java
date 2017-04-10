package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

import java.lang.reflect.Type;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.NoOpBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;

import com.google.common.base.Joiner;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class BatchInformationTypeAdapter implements JsonSerializer<BatchInformation>, JsonDeserializer<BatchInformation> {

    private static final Logger log = Logger.getLogger(BatchInformationTypeAdapter.class);
    private static final BatchInformationTypeAdapterFactory factory = new BatchInformationTypeAdapterFactory();

    @Override
    public BatchInformation deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2) throws JsonParseException {
        try {
            JsonObject json = arg0.getAsJsonObject();
            String type = json.get("class").getAsString();
            JsonDeserializer<? extends BatchInformation> deserializer = factory.getDeserializerFromName(type);
            return deserializer.deserialize(arg0, arg1, arg2);
        } catch (Exception e) {
            log.trace("Unable to deserialize JsonElement: " + arg0);
            log.trace("Returning an empty Batch");
            return new NoOpBatchInformation();
        }
    }

    @Override
    public JsonElement serialize(BatchInformation batch, Type arg1, JsonSerializationContext arg2) {
        JsonSerializer<? extends BatchInformation> serializer = factory.getSerializerFromName(batch.getClass().getName());
        
        if(batch instanceof SpanBatchInformation) {
            return ((SpanBatchInformationTypeAdapter) serializer).serialize((SpanBatchInformation) batch, arg1, arg2);
        } else {
            return ((JoinBatchInformationTypeAdapter) serializer).serialize((JoinBatchInformation) batch, arg1, arg2);
        }
    }

}