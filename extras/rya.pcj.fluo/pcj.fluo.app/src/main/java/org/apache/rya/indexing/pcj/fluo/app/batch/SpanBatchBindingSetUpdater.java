package org.apache.rya.indexing.pcj.fluo.app.batch;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

import com.google.common.base.Preconditions;

public class SpanBatchBindingSetUpdater extends AbstractBatchBindingSetUpdater {

    private static final Logger log = Logger.getLogger(SpanBatchBindingSetUpdater.class);

    @Override
    public void processBatch(TransactionBase tx, Bytes row, BatchInformation batch) throws Exception {
        super.processBatch(tx, row, batch);
        Preconditions.checkArgument(batch instanceof SpanBatchDeleteInformation);
        SpanBatchDeleteInformation spanBatch = (SpanBatchDeleteInformation) batch;
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
            log.trace("Batch size met.  There are remaining results that need to be deleted.  Creating a new batch of size: "
                    + spanBatch.getBatchSize() + " with Span: " + newSpan + " and Column: " + column);
            spanBatch.setSpan(newSpan);
            BatchInformationDAO.addBatch(tx, BatchRowKeyUtil.getNodeId(row), spanBatch);
        }
    }

    private Optional<RowColumn> deleteBatch(TransactionBase tx, Span span, Column column, int batchSize) {

        log.trace("Deleting batch of size: " + batchSize + " using Span: " + span + " and Column: " + column);
        RowScanner rs = tx.scanner().over(span).fetch(column).byRow().build();
        try {
            Iterator<ColumnScanner> colScannerIter = rs.iterator();

            int count = 0;
            boolean batchLimitMet = false;
            Bytes row = span.getStart().getRow();
            while (colScannerIter.hasNext() && !batchLimitMet) {
                ColumnScanner colScanner = colScannerIter.next();
                row = colScanner.getRow();
                Iterator<ColumnValue> iter = colScanner.iterator();
                while (iter.hasNext()) {
                    if (count >= batchSize) {
                        batchLimitMet = true;
                        break;
                    }
                    ColumnValue colVal = iter.next();
                    tx.delete(row, colVal.getColumn());
                    count++;
                }
            }

            if (batchLimitMet) {
                return Optional.of(new RowColumn(row));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
