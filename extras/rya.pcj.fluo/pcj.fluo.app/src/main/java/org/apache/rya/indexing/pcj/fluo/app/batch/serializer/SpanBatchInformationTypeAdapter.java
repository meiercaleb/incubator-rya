package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

import java.lang.reflect.Type;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchInformation;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class SpanBatchInformationTypeAdapter implements JsonSerializer<SpanBatchInformation>, JsonDeserializer<SpanBatchInformation> {

    @Override
    public SpanBatchInformation deserialize(JsonElement element, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject json = element.getAsJsonObject();
        int batchSize = json.get("batchSize").getAsInt();
        String[] colArray = json.get("column").getAsString().split("\u0000");
        Column column = new Column(colArray[0], colArray[1]);
        String[] rows = json.get("span").getAsString().split("\u0000");
        boolean startInc = json.get("startInc").getAsBoolean();
        boolean endInc = json.get("endInc").getAsBoolean();
        Span span = new Span(new RowColumn(rows[0], column), startInc, new RowColumn(rows[1], column), endInc);
        return SpanBatchInformation.builder().setBatchSize(batchSize).setSpan(span).setColumn(column).build();
    }

    @Override
    public JsonElement serialize(SpanBatchInformation batch, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add("class", new JsonPrimitive(batch.getClass().getName()));
        result.add("batchSize", new JsonPrimitive(batch.getBatchSize()));
        Column column = batch.getColumn();
        result.add("column", new JsonPrimitive(column.getsFamily() + "\u0000" + column.getsQualifier()));
        Span span = batch.getSpan();
        result.add("span", new JsonPrimitive(span.getStart().getsRow() + "\u0000" + span.getEnd().getsRow()));
        result.add("startInc", new JsonPrimitive(span.isStartInclusive()));
        result.add("endInc", new JsonPrimitive(span.isEndInclusive()));
        return result;
    }

}
