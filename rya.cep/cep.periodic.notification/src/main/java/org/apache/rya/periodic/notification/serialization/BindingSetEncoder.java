package org.apache.rya.periodic.notification.serialization;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Joiner;
import com.google.common.primitives.Bytes;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

public class BindingSetEncoder implements Encoder<BindingSet>, Decoder<BindingSet> {

    private static final Logger log = Logger.getLogger(BindingSetEncoder.class);
    private static final AccumuloPcjSerializer serializer =  new AccumuloPcjSerializer();
    private static final byte[] DELIM_BYTE = "\u0002".getBytes();
    
    @Override
    public byte[] toBytes(BindingSet bindingSet) {
        try {
            return getBytes(getVarOrder(bindingSet), bindingSet);
        } catch(Exception e) {
            log.trace("Unable to serialize BindingSet: " + bindingSet);
            return new byte[0];
        }
    }

    @Override
    public BindingSet fromBytes(byte[] bsBytes) {
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

}
