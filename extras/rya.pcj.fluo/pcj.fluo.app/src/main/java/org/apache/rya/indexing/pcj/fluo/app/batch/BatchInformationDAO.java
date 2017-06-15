package org.apache.rya.indexing.pcj.fluo.app.batch;

import java.util.Optional;

import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

public class BatchInformationDAO {
    
    public static void addBatch(TransactionBase tx, String nodeId, BatchInformation batch) {
        Bytes row = BatchRowKeyUtil.getRow(nodeId);
        tx.set(row, FluoQueryColumns.BATCH_COLUMN, Bytes.of(BatchInformationSerializer.toBytes(batch)));
    }
    
    public static Optional<BatchInformation> getBatchInformation(TransactionBase tx, Bytes row) {
        Bytes val = tx.get(row, FluoQueryColumns.BATCH_COLUMN);
        if(val != null) {
            return BatchInformationSerializer.fromBytes(val.toArray());
        } else {
            return Optional.empty();
        }
    }
    
}
