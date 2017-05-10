package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Optional;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

public class BatchInformationSerializerTest {

    @Test
    public void testSpanBatchInformationSerialization() {

        SpanBatchDeleteInformation batch = SpanBatchDeleteInformation.builder().setBatchSize(1000)
                .setColumn(FluoQueryColumns.PERIODIC_QUERY_BINDING_SET).setSpan(Span.prefix(Bytes.of("prefix"))).build();
        System.out.println(batch);
        byte[] batchBytes = BatchInformationSerializer.toBytes(batch);
        Optional<BatchInformation> decodedBatch = BatchInformationSerializer.fromBytes(batchBytes);
        System.out.println(decodedBatch);
        assertEquals(batch, decodedBatch.get());
    }

    @Test
    public void testJoinBatchInformationSerialization() {

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("a", new URIImpl("urn:123"));
        bs.addBinding("b", new URIImpl("urn:456"));
        VisibilityBindingSet vBis = new VisibilityBindingSet(bs, "FOUO");
        
        JoinBatchInformation batch = JoinBatchInformation.builder().setBatchSize(1000).setTask(Task.Update)
                .setColumn(FluoQueryColumns.PERIODIC_QUERY_BINDING_SET).setSpan(Span.prefix(Bytes.of("prefix346")))
                .setJoinType(JoinType.LEFT_OUTER_JOIN).setSide(Side.RIGHT).setVarOrder(new VariableOrder(Arrays.asList("a", "b")))
                .setBs(vBis).build();
        
        System.out.println(batch);
        byte[] batchBytes = BatchInformationSerializer.toBytes(batch);
        Optional<BatchInformation> decodedBatch = BatchInformationSerializer.fromBytes(batchBytes);
        System.out.println(decodedBatch);
        assertEquals(batch, decodedBatch.get());
    }

}
