package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.serialization.kryo.RyaSubGraphSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RyaSubGraphKafkaSerDe implements Serializer<RyaSubGraph>, Deserializer<RyaSubGraph> {

    private final Kryo kryo;
    
    public RyaSubGraphKafkaSerDe() {
        kryo = new Kryo();
        kryo.register(RyaSubGraph.class,new RyaSubGraphSerializer());
    }
    
    public RyaSubGraph fromBytes(byte[] bundleBytes) {
        return kryo.readObject(new Input(bundleBytes), RyaSubGraph.class);
    }

    public byte[] toBytes(RyaSubGraph bundle) {
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, bundle, new RyaSubGraphSerializer());
        return output.getBuffer();
    }

    @Override
    public RyaSubGraph deserialize(String arg0, byte[] bundleBytes) {
        return fromBytes(bundleBytes);
    }


    @Override
    public byte[] serialize(String arg0, RyaSubGraph subgraph) {
        return toBytes(subgraph);
    }

    @Override
    public void close() {
    }
    
    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }
}
