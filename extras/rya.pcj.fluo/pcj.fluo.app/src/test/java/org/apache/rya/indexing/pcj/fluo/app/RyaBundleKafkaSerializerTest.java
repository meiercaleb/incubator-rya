package org.apache.rya.indexing.pcj.fluo.app;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerializer;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

public class RyaBundleKafkaSerializerTest {

    private static final RyaSubGraphKafkaSerializer serializer = new RyaSubGraphKafkaSerializer();
    
    @Test
    public void serializationTestWithURI() {
        RyaSubGraph bundle = new RyaSubGraph(UUID.randomUUID().toString());
        bundle.addStatement(new RyaStatement(new RyaURI("uri:123"), new RyaURI("uri:234"), new RyaURI("uri:345")));
        bundle.addStatement(new RyaStatement(new RyaURI("uri:345"), new RyaURI("uri:567"), new RyaURI("uri:789")));
        byte[] bundleBytes = serializer.toBytes(bundle);
        RyaSubGraph deserializedBundle = serializer.fromBytes(bundleBytes);
        assertEquals(bundle, deserializedBundle);
    }
    
    
    @Test
    public void serializationTestWithLiteral() {
        RyaSubGraph bundle = new RyaSubGraph(UUID.randomUUID().toString());
        bundle.addStatement(new RyaStatement(new RyaURI("uri:123"), new RyaURI("uri:234"), new RyaType(XMLSchema.INTEGER, "345")));
        bundle.addStatement(new RyaStatement(new RyaURI("uri:345"), new RyaURI("uri:567"), new RyaType(XMLSchema.INTEGER, "789")));
        byte[] bundleBytes = serializer.toBytes(bundle);
        RyaSubGraph deserializedBundle = serializer.fromBytes(bundleBytes);
        assertEquals(bundle, deserializedBundle);
    }
    
}
