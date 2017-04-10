package org.apache.rya.indexing.pcj.fluo.app.batch;

import java.util.Iterator;
import java.util.Optional;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Preconditions;

public class SpanBatchBindingSetUpdater extends AbstractBatchBindingSetUpdater {

    private static final Logger log = Logger.getLogger(SpanBatchBindingSetUpdater.class);

    @Override
    public void processBatch(TransactionBase tx, String nodeId, BatchInformation batch) {

        Preconditions.checkArgument(batch instanceof SpanBatchInformation);
        SpanBatchInformation spanBatch = (SpanBatchInformation) batch;
        Task task = spanBatch.getTask();
        int batchSize = spanBatch.getBatchSize();
        Span span = spanBatch.getSpan();
        Column column = batch.getColumn();
        Optional<RowColumn> rowCol = Optional.empty();

        switch (task) {
        case Add:
            log.trace("The Task Add is not supported for SpanBatchBindingSetUpdater.  Batch " + batch + " will not be processed.");
            break;
        case Delete:
            rowCol = deleteBatch(tx, span, column, batchSize);
            break;
        case Update:
            log.trace("The Task Update is not supported for SpanBatchBindingSetUpdater.  Batch " + batch + " will not be processed.");
            break;
        default:
            log.trace("Invalid Task type.  Aborting batch operation.");
            break;
        }

        if (rowCol.isPresent()) {
            Span newSpan = getNewSpan(rowCol.get(), spanBatch.getSpan());
            spanBatch.setSpan(newSpan);
            tx.set(Bytes.of(nodeId), FluoQueryColumns.BATCH_COLUMN, Bytes.of(BatchInformationSerializer.toBytes(spanBatch)));
        }
    }

    private Optional<RowColumn> deleteBatch(TransactionBase tx, Span span, Column column, int batchSize) {

        RowScanner rs = tx.scanner().over(span).fetch(column).byRow().build();
        Iterator<ColumnScanner> colScannerIter = rs.iterator();

        int count = 0;
        boolean batchLimitMet = false;
        Bytes row = span.getStart().getRow();
        while (colScannerIter.hasNext()) {
            ColumnScanner colScanner = colScannerIter.next();
            row = colScanner.getRow();
            Iterator<ColumnValue> iter = colScanner.iterator();
            while (iter.hasNext()) {
                if(count >= batchSize) {
                    batchLimitMet = true;
                    break;
                }
                ColumnValue colVal = iter.next();
                tx.delete(row, colVal.getColumn());
                count++;
            }
        }

        if (batchLimitMet) {
            return Optional.of(new RowColumn(row, column));
        } else {
            return Optional.empty();
        }
    }

}
