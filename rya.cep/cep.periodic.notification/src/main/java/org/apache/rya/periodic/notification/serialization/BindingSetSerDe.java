package org.apache.rya.periodic.notification.serialization;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Joiner;
import com.google.common.primitives.Bytes;

public class BindingSetSerDe implements Serializer<BindingSet>, Deserializer<BindingSet> {

    private static final Logger log = Logger.getLogger(BindingSetSerDe.class);
    private static final AccumuloPcjSerializer serializer =  new AccumuloPcjSerializer();
    private static final byte[] DELIM_BYTE = "\u0002".getBytes();
    
    private byte[] toBytes(BindingSet bindingSet) {
        try {
            return getBytes(getVarOrder(bindingSet), bindingSet);
        } catch(Exception e) {
            log.trace("Unable to serialize BindingSet: " + bindingSet);
            return new byte[0];
        }
    }

    private BindingSet fromBytes(byte[] bsBytes) {
        try{
        int firstIndex = Bytes.indexOf(bsBytes, DELIM_BYTE);
        byte[] varOrderBytes = Arrays.copyOf(bsBytes, firstIndex);
        byte[] bsBytesNoVarOrder = Arrays.copyOfRange(bsBytes, firstIndex + 1, bsBytes.length);
        VariableOrder varOrder = new VariableOrder(new String(varOrderBytes,"UTF-8").split(";"));
        return getBindingSet(varOrder, bsBytesNoVarOrder);
        } catch(Exception e) {
            log.trace("Unable to deserialize BindingSet: " + bsBytes);
            return new QueryBindingSet();
        }
    }
    
    private VariableOrder getVarOrder(BindingSet bs) {
        return new VariableOrder(bs.getBindingNames());
    }
    
    private byte[] getBytes(VariableOrder varOrder, BindingSet bs) throws UnsupportedEncodingException, BindingSetConversionException {
        byte[] bsBytes = serializer.convert(bs, varOrder);
        String varOrderString = Joiner.on(";").join(varOrder.getVariableOrders());
        byte[] varOrderBytes = varOrderString.getBytes("UTF-8");
        return Bytes.concat(varOrderBytes, DELIM_BYTE, bsBytes);
    }
    
    private BindingSet getBindingSet(VariableOrder varOrder, byte[] bsBytes) throws BindingSetConversionException {
        return serializer.convert(bsBytes, varOrder);
    }

    @Override
    public BindingSet deserialize(String topic, byte[] bytes) {
        return fromBytes(bytes);
    }

    @Override
    public void close() {
        // Do nothing. Nothing to close.
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // Do nothing.  Nothing to configure.
    }

    @Override
    public byte[] serialize(String topic, BindingSet bs) {
        return toBytes(bs);
    }

}
