package org.apache.rya.api.domain.serialization.kryo;


import org.openrdf.model.impl.URIImpl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;

public class RyaStatementSerializer extends Serializer<RyaStatement> {
    
    public static void writeToKryo(Kryo kryo, Output output, RyaStatement object) {
        output.writeString(object.getSubject().getData());
        output.writeString(object.getPredicate().getData());
        output.writeString(object.getObject().getDataType().toString());
        output.writeString(object.getObject().getData());
        boolean hasContext = object.getContext() != null;
        output.writeBoolean(hasContext);
        if(hasContext){
            output.writeString(object.getContext().getData());
        }
        boolean shouldWrite = object.getColumnVisibility() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeInt(object.getColumnVisibility().length);
            output.writeBytes(object.getColumnVisibility());
        }
        shouldWrite = object.getQualifer() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeString(object.getQualifer());
        }
        shouldWrite = object.getTimestamp() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeLong(object.getTimestamp());
        }
        shouldWrite = object.getValue() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeBytes(object.getValue());
        }
    }   

    @Override
    public void write(Kryo kryo, Output output, RyaStatement object) {
        writeToKryo(kryo, output, object);
    }
    
    public static RyaStatement readFromKryo(Kryo kryo, Input input, Class<RyaStatement> type){
        String subject = input.readString();
        String predicate = input.readString();
        String objectType = input.readString();
        String objectValue = input.readString();
        RyaType value;
        if (objectType.equals("http://www.w3.org/2001/XMLSchema#anyURI")){
            value = new RyaURI(objectValue);
        }
        else {
            value = new RyaType(new URIImpl(objectType), objectValue);
        }
        RyaStatement statement = new RyaStatement(new RyaURI(subject), new RyaURI(predicate), value);
        int length = 0;
        boolean hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setContext(new RyaURI(input.readString()));
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            length = input.readInt();
            statement.setColumnVisibility(input.readBytes(length));
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setQualifer(input.readString());
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setTimestamp(input.readLong());
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            length = input.readInt();
            statement.setValue(input.readBytes(length));
        }

        return statement;
    }

    public static RyaStatement read(Input input){
        String subject = input.readString();
        String predicate = input.readString();
        String objectType = input.readString();
        String objectValue = input.readString();
        RyaType value;
        if (objectType.equals("http://www.w3.org/2001/XMLSchema#anyURI")){
            value = new RyaURI(objectValue);
        }
        else {
            value = new RyaType(new URIImpl(objectType), objectValue);
        }
        RyaStatement statement = new RyaStatement(new RyaURI(subject), new RyaURI(predicate), value);
        int length = 0;
        boolean hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setContext(new RyaURI(input.readString()));
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            length = input.readInt();
            statement.setColumnVisibility(input.readBytes(length));
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setQualifer(input.readString());
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setTimestamp(input.readLong());
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            length = input.readInt();
            statement.setValue(input.readBytes(length));
        }

        return statement;
    }

    @Override
    public RyaStatement read(Kryo kryo, Input input, Class<RyaStatement> type) {
        return readFromKryo(kryo, input, type);
    }

}
