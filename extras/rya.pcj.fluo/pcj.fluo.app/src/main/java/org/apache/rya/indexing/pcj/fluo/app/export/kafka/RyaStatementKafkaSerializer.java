package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import java.io.ByteArrayOutputStream;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.serialization.kryo.RyaStatementSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

public class RyaStatementKafkaSerializer implements Encoder<RyaStatement>, Decoder<RyaStatement> {

 private final Kryo kryo;
    
    public RyaStatementKafkaSerializer() {
        kryo = new Kryo();
        kryo.register(RyaStatement.class,new RyaStatementSerializer());
    }
    
    @Override
    public RyaStatement fromBytes(byte[] statementBytes) {
        return kryo.readObject(new Input(statementBytes), RyaStatement.class);
    }

    @Override
    public byte[] toBytes(RyaStatement statement) {
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, statement, new RyaStatementSerializer());
        return output.getBuffer();
    }
}
