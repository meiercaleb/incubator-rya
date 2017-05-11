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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.util.BindingSetUtil;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Updates the results of a Query node when one of its children has added a
 * new Binding Set to its results.
 */
@DefaultAnnotation(NonNull.class)
public class QueryResultUpdater {
    private static final Logger log = Logger.getLogger(QueryResultUpdater.class);

    private static final FluoQueryMetadataDAO METADATA_DA0 = new FluoQueryMetadataDAO();
    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    /**
     * Updates the results of a Query node when one of its children has added a
     * new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childBindingSet - A binding set that the query's child node has emmitted. (not null)
     * @param queryMetadata - The metadata of the Query whose results will be updated. (not null)
     * @throws Exception A problem caused the update to fail.
     */
    public void updateQueryResults(
            final TransactionBase tx,
            final VisibilityBindingSet childBindingSet,
            final QueryMetadata queryMetadata) throws Exception {
        checkNotNull(tx);
        checkNotNull(childBindingSet);
        checkNotNull(queryMetadata);

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                "Join Node ID: " + queryMetadata.getNodeId() + "\n" +
                "Child Node ID: " + queryMetadata.getChildNodeId() + "\n" +
                "Child Binding Set:\n" + childBindingSet + "\n");

        // Create the query's Binding Set from the child node's binding set.
        final VariableOrder queryVarOrder = queryMetadata.getVariableOrder();
        final BindingSet queryBindingSet = BindingSetUtil.keepBindings(queryVarOrder, childBindingSet);

        // Create the Row Key for the result. If the child node groups results, then the key must only contain the Group By variables.
        final Bytes resultRow;

        final String childNodeId = queryMetadata.getChildNodeId();
        final boolean isGrouped = childNodeId.startsWith( IncrementalUpdateConstants.AGGREGATION_PREFIX );
        if(isGrouped) {
            final AggregationMetadata aggMetadata = METADATA_DA0.readAggregationMetadata(tx, childNodeId);
            final VariableOrder groupByVars = aggMetadata.getGroupByVariableOrder();
            resultRow = RowKeyUtil.makeRowKey(queryMetadata.getNodeId(), groupByVars, queryBindingSet);
        } else {
            resultRow = RowKeyUtil.makeRowKey(queryMetadata.getNodeId(), queryVarOrder, queryBindingSet);
        }

        // Create the Binding Set that goes in the Node Value. It does contain visibilities.
        final Bytes nodeValueBytes = BS_SERDE.serialize(childBindingSet);

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                "New Binding Set: " + childBindingSet + "\n");

        tx.set(resultRow, FluoQueryColumns.QUERY_BINDING_SET, nodeValueBytes);
    }
}