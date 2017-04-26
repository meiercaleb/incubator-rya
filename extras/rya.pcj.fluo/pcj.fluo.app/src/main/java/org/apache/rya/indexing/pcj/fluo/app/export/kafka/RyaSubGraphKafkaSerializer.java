package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import java.io.ByteArrayOutputStream;

import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.serialization.kryo.RyaSubGraphSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

public class RyaSubGraphKafkaSerializer implements Encoder<RyaSubGraph>, Decoder<RyaSubGraph> {

    private final Kryo kryo;
    
    public RyaSubGraphKafkaSerializer() {
        kryo = new Kryo();
        kryo.register(RyaSubGraph.class,new RyaSubGraphSerializer());
    }
    
    @Override
    public RyaSubGraph fromBytes(byte[] bundleBytes) {
        return kryo.readObject(new Input(bundleBytes), RyaSubGraph.class);
    }

    @Override
    public byte[] toBytes(RyaSubGraph bundle) {
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, bundle, new RyaSubGraphSerializer());
        return output.getBuffer();
    }

}
