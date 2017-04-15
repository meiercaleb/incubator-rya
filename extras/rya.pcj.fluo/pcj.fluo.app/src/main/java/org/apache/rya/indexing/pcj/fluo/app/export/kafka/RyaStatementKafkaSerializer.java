package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.fluo.app.RyaStatementSerializer;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

public class RyaStatementKafkaSerializer implements Encoder<RyaStatement>, Decoder<RyaStatement> {

    private static final Logger log = Logger.getLogger(RyaStatementKafkaSerializer.class);
    
    @Override
    public RyaStatement fromBytes(byte[] arg0) {
        try {
            return RyaStatementSerializer.deserialize(arg0);
        } catch (Exception e) {
            log.trace("Unable to deserialize message.");
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toBytes(RyaStatement arg0) {
        try {
            return RyaStatementSerializer.serialize(arg0);
        } catch (Exception e) {
           log.trace("Unable to serialize RyaStatement: " + arg0);
           throw new RuntimeException(e);
        }
    }


}
