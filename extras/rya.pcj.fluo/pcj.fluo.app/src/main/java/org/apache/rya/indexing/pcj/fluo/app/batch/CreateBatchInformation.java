package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

public class CreateBatchInformation {
    
    public static void createBatch(TransactionBase tx, String nodeId, BatchInformation batch) {
        tx.set(Bytes.of(nodeId), FluoQueryColumns.BATCH_COLUMN, Bytes.of(BatchInformationSerializer.toBytes(batch)));
    }
    
}
