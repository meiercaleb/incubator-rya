package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

import java.util.Map;

import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchInformation;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;

public class BatchInformationTypeAdapterFactory {

    public JsonSerializer<? extends BatchInformation> getSerializerFromName(String name) {
        return serializers.get(name);
    }
    
    public JsonDeserializer<? extends BatchInformation> getDeserializerFromName(String name) {
        return deserializers.get(name);
    }
    
    static final Map<String, JsonSerializer<? extends BatchInformation>> serializers = ImmutableMap.of(
            SpanBatchInformation.class.getName(), new SpanBatchInformationTypeAdapter(),
            JoinBatchInformation.class.getName(), new JoinBatchInformationTypeAdapter()
        );
    
    static final Map<String, JsonDeserializer<? extends BatchInformation>> deserializers = ImmutableMap.of(
            SpanBatchInformation.class.getName(), new SpanBatchInformationTypeAdapter(),
            JoinBatchInformation.class.getName(), new JoinBatchInformationTypeAdapter()
        );
    
}
