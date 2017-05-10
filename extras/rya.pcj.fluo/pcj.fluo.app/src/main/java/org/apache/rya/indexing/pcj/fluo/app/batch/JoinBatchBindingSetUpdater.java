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
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.IterativeJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.LeftOuterJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.NaturalJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

/*
 * Performs updates to BindingSets in the JoinBindingSet column in batch fashion.
 * 
 */
public class JoinBatchBindingSetUpdater extends AbstractBatchBindingSetUpdater {

    private static final Logger log = Logger.getLogger(JoinBatchBindingSetUpdater.class);
    private static final VisibilityBindingSetStringConverter value_converter = new VisibilityBindingSetStringConverter();
    private static final BindingSetStringConverter row_converter = new BindingSetStringConverter();
    private static final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

    /**
     * Processes {@link JoinBatchInformation}. Updates the BindingSets
     * associated with the specified nodeId. The BindingSets are processed in
     * batch fashion, where the number of results is indicated by
     * {@link JoinBatchInformation#getBatchSize()}. BindingSets are either
     * Added, Deleted, or Updated according to
     * {@link JoinBatchInformation#getTask()}. In the event that the number of
     * entries that need to be updated exceeds the batch size, the row of the
     * first unprocessed BindingSets is used to create a new JoinBatch job to
     * process the remaining BindingSets.
     */
    @Override
    public void processBatch(TransactionBase tx, String nodeId, BatchInformation batch) {
        super.processBatch(tx, nodeId, batch);
        Preconditions.checkArgument(batch instanceof JoinBatchInformation);
        JoinBatchInformation joinBatch = (JoinBatchInformation) batch;
        Task task = joinBatch.getTask();

        // Figure out which join algorithm we are going to use.
        final IterativeJoin joinAlgorithm;
        switch (joinBatch.getJoinType()) {
        case NATURAL_JOIN:
            joinAlgorithm = new NaturalJoin();
            break;
        case LEFT_OUTER_JOIN:
            joinAlgorithm = new LeftOuterJoin();
            break;
        default:
            throw new RuntimeException("Unsupported JoinType: " + joinBatch.getJoinType());
        }

        Set<VisibilityBindingSet> bsSet = new HashSet<>();
        Optional<RowColumn> rowCol = fillSiblingBatch(tx, joinBatch, bsSet);

        // Iterates over the resulting BindingSets from the join.
        final Iterator<VisibilityBindingSet> newJoinResults;
        VisibilityBindingSet bs = joinBatch.getBs();
        if (joinBatch.getSide() == Side.LEFT) {
            newJoinResults = joinAlgorithm.newLeftResult(bs, bsSet.iterator());
        } else {
            newJoinResults = joinAlgorithm.newRightResult(bsSet.iterator(), bs);
        }

        // Insert the new join binding sets to the Fluo table.
        final VariableOrder joinVarOrder = dao.readJoinMetadata(tx, nodeId).getVariableOrder();
        while (newJoinResults.hasNext()) {
            final BindingSet newJoinResult = newJoinResults.next();
            final String joinBindingSetStringId = row_converter.convert(newJoinResult, joinVarOrder);
            final String joinBindingSetStringValue = value_converter.convert(newJoinResult, joinVarOrder);
            final String row = nodeId + NODEID_BS_DELIM + joinBindingSetStringId;
            final Column col = FluoQueryColumns.JOIN_BINDING_SET;
            final String value = joinBindingSetStringValue;
            processTask(tx, task, row, col, value);
        }

        // if batch limit met, there are additional entries to process
        // update the span and register updated batch job
        if (rowCol.isPresent()) {
            Span newSpan = getNewSpan(rowCol.get(), joinBatch.getSpan());
            joinBatch.setSpan(newSpan);
            tx.set(Bytes.of(nodeId), FluoQueryColumns.BATCH_COLUMN, Bytes.of(BatchInformationSerializer.toBytes(joinBatch)));
        }

    }

    private void processTask(TransactionBase tx, Task task, String row, Column column, String value) {
        switch (task) {
        case Add:
            tx.set(row, column, value);
            break;
        case Delete:
            tx.delete(Bytes.of(row), column);
            break;
        case Update:
            log.trace("The Task Update is not supported for JoinBatchBindingSetUpdater.  Batch will not be processed.");
            break;
        default:
            log.trace("Invalid Task type.  Aborting batch operation.");
            break;
        }
    }

    /**
     * Fetches batch to be processed by scanning over the Span specified by the
     * {@link JoinBatchInformation}. The number of results is less than or equal
     * to the batch size specified by the JoinBatchInformation.
     * 
     * @param tx - Fluo transaction in which batch operation is performed
     * @param batch - batch order to be processed
     * @param bsSet- set that batch results are added to
     * @return Set - containing results of sibling scan.
     */
    private Optional<RowColumn> fillSiblingBatch(TransactionBase tx, JoinBatchInformation batch, Set<VisibilityBindingSet> bsSet) {

        Span span = batch.getSpan();
        Column column = batch.getColumn();
        int batchSize = batch.getBatchSize();
        VariableOrder varOrder = batch.getVarOrder();

        RowScanner rs = tx.scanner().over(span).fetch(column).byRow().build();
        Iterator<ColumnScanner> colScannerIter = rs.iterator();

        boolean batchLimitMet = false;
        Bytes row = span.getStart().getRow();
        while (colScannerIter.hasNext() && !batchLimitMet) {
            ColumnScanner colScanner = colScannerIter.next();
            row = colScanner.getRow();
            Iterator<ColumnValue> iter = colScanner.iterator();
            while (iter.hasNext()) {
                if (bsSet.size() >= batchSize) {
                    batchLimitMet = true;
                    break;
                }
                bsSet.add(value_converter.convert(iter.next().getsValue(), varOrder));
            }
        }

        if (batchLimitMet) {
            return Optional.of(new RowColumn(row, column));
        } else {
            return Optional.empty();
        }
    }
}
