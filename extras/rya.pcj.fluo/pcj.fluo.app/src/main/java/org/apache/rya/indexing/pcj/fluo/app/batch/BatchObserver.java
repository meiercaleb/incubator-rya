package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

/**
 * BatchObserver processes tasks that need to be broken into batches. Entries
 * stored stored in this {@link FluoQueryColumns#BATCH_COLUMN} are of the form
 * Row: nodeId, Value: BatchInformation. The nodeId indicates the node that the
 * batch operation will be performed on. All batch operations are performed on
 * the bindingSet column for the {@link NodeType} corresponding to the given nodeId. For
 * example, if the nodeId indicated that the NodeType was StatementPattern, then
 * the batch operation would be performed on
 * {@link FluoQueryColumns#STATEMENT_PATTERN_BINDING_SET}.
 */
public class BatchObserver extends AbstractObserver {

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
        BatchInformation batchInfo = parseBatchInformation(tx, row, col);
        batchInfo.getBatchUpdater().processBatch(tx, row.toString(), batchInfo);
    }

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.BATCH_COLUMN, NotificationType.STRONG);
    }

    private BatchInformation parseBatchInformation(final TransactionBase tx, Bytes row, Column col) {
        Bytes value = tx.get(row, col);
        return BatchInformationSerializer.fromBytes(value.toArray());
    }
}
