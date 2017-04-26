package org.apache.rya.api.domain.serialization.kryo;

import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.RyaStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RyaSubGraphSerializer extends Serializer<RyaSubGraph>{
    static final Logger log = LoggerFactory.getLogger(RyaSubGraphSerializer.class);

    @Override
    public void write(Kryo kryo, Output output, RyaSubGraph object) {
        output.writeString(object.getId());
        output.writeInt(object.getStatements().size());
        for (RyaStatement statement : object.getStatements()){
            RyaStatementSerializer.writeToKryo(kryo, output, statement);
        }
    }
    
    public static RyaSubGraph read(Input input){
        RyaSubGraph bundle = new RyaSubGraph(input.readString());
        int numStatements = input.readInt();
        for (int i=0; i < numStatements; i++){
            bundle.addStatement(RyaStatementSerializer.read(input));
        }       
        return bundle;
    }

    @Override
    public RyaSubGraph read(Kryo kryo, Input input, Class<RyaSubGraph> type) {
        RyaSubGraph bundle = new RyaSubGraph(input.readString());
        int numStatements = input.readInt();
        
        for (int i=0; i < numStatements; i++){
            bundle.addStatement(RyaStatementSerializer.readFromKryo(kryo, input, RyaStatement.class));
        }       
        return bundle;
    }

}