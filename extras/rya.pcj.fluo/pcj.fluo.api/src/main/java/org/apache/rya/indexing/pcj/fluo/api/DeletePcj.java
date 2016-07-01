package org.apache.rya.indexing.pcj.fluo.api;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.TypedTransaction;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.StringTypeLayer;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.openrdf.query.BindingSet;

/**
 * Deletes a Pre-computed Join (PCJ) from Fluo.
 * <p>
 * This is a two phase process.
 * <ol>
 * <li>Delete metadata about each node of the query using a single Fluo
 * transaction. This prevents new {@link BindingSet}s from being created when
 * new triples are inserted.</li>
 * <li>Delete BindingSets associated with each node of the query. This is done
 * in a batch fashion to guard against large delete transactions that don't fit
 * into memory.</li>
 * </ol>
 */

public class DeletePcj {

    private int batchSize;
    private final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

    public DeletePcj(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * This method deletes all metadata and {@link BindingSet}s associated with
     * a specified pcjId from the Fluo table associated with a specified
     * Transaction object.
     *
     * @param client
     *            - FluoClient for a given Fluo table
     * @param pcjId
     *            - id for a query whose data will be deleted
     */
    public void deletePcj(FluoClient client, String pcjId) {

        Transaction tx = client.newTransaction();
        List<String> nodeIds = getNodeIds(tx, pcjId);
        deleteMetaData(tx, nodeIds, pcjId);

        for (String nodeId : nodeIds) {
            deleteData(client, nodeId);
        }

    }

    /**
     * This method retrieves all of the nodeIds that are part of the query with
     * specified pcjId.
     *
     * @param tx
     *            - Transaction of a given Fluo table
     * @param pcjId
     *            - id of query
     * @return - list of nodeIds associated with the query pcjId
     */
    private List<String> getNodeIds(Transaction tx, String pcjId) {
        String queryId = getQueryIdFromPcjId(tx, pcjId);

        List<String> ids = new ArrayList<>();
        ids.add(queryId);
        getChildNodeIds(tx, queryId, ids);
        return ids;

    }

    /**
     * Recursively navigate query tree to extract all of the nodeIds
     *
     * @param dao
     *            - dao to retrieve node metadata
     * @param tx
     *            - Transaction of a given Fluo table
     * @param nodeId
     *            - current node in query tree
     * @param ids
     *            - nodeIds extracted from query tree
     */
    private void getChildNodeIds(Transaction tx, String nodeId, List<String> ids) {

        NodeType type = NodeType.fromNodeId(nodeId).get();
        switch (type) {
        case QUERY:
            QueryMetadata queryMeta = dao.readQueryMetadata(tx, nodeId);
            String queryChild = queryMeta.getChildNodeId();
            ids.add(queryChild);
            getChildNodeIds(tx, queryChild, ids);
            break;
        case JOIN:
            JoinMetadata joinMeta = dao.readJoinMetadata(tx, nodeId);
            String lchild = joinMeta.getLeftChildNodeId();
            String rchild = joinMeta.getRightChildNodeId();
            ids.add(lchild);
            ids.add(rchild);
            getChildNodeIds(tx, lchild, ids);
            getChildNodeIds(tx, rchild, ids);
            break;
        case FILTER:
            FilterMetadata filterMeta = dao.readFilterMetadata(tx, nodeId);
            String filterChild = filterMeta.getChildNodeId();
            ids.add(filterChild);
            getChildNodeIds(tx, filterChild, ids);
            break;
        case STATEMENT_PATTERN:
            break;
        }
    }

    /**
     * Deletes metadata for all nodeIds associated with a given queryId in a
     * single transaction. Prevents additional BindingSets from being created as
     * new triples are added.
     *
     * @param tx
     *            - Transaction of a given Fluo table
     * @param nodeIds
     *            - nodes whose metatdata will be deleted
     */
    private void deleteMetaData(Transaction tx, List<String> nodeIds, String pcjId) {

        try (TypedTransaction typeTx = new StringTypeLayer().wrap(tx)) {

            deletePcjIdAndSparqlMetadata(typeTx, pcjId);
            for (String nodeId : nodeIds) {
                NodeType type = NodeType.fromNodeId(nodeId).get();
                deleteMetadataColumns(typeTx, type.getMetaDataColumns(), nodeId);
            }
            typeTx.commit();
        }

    }

    /**
     * Deletes all metadata for QueryId and its children
     *
     * @param tx
     * @param columns
     * @param nodeId
     */
    private void deleteMetadataColumns(TypedTransaction tx,
            List<Column> columns, String nodeId) {

        Bytes row = Bytes.of(nodeId);
        for (Column column : columns) {
            tx.delete(row, column);
        }
    }


    /**
     * Deletes high level query meta for converting from queryId to pcjId and vice
     * versa, as well as converting from sparql to queryId
     * @param tx
     * @param pcjId
     */
    private void deletePcjIdAndSparqlMetadata(TypedTransaction tx, String pcjId) {
        String queryId = getQueryIdFromPcjId(tx, pcjId);
        String sparql = getSparqlFromQueryId(tx, queryId);
        tx.delete(Bytes.of(queryId), FluoQueryColumns.RYA_PCJ_ID);
        tx.delete(Bytes.of(sparql), FluoQueryColumns.QUERY_ID);
        tx.delete(Bytes.of(pcjId), FluoQueryColumns.PCJ_ID_QUERY_ID);
    }


    /**
     * Deletes all BindingSets associated with the specified nodeId.
     *
     * @param nodeId
     *            - nodeId whose BindingSets will be deleted
     * @param client
     *            - FluoClient for a given Fluo table
     */
    private void deleteData(FluoClient client, String nodeId) {

        NodeType type = NodeType.fromNodeId(nodeId).get();
        Transaction tx = client.newTransaction();
        while(deleteDataBatch(tx, getIterator(tx, nodeId, type.getBsColumn()), type.getBsColumn())) {
            tx = client.newTransaction();
        }
    }

    private RowIterator getIterator(Transaction tx, String nodeId, Column column) {

        ScannerConfiguration sc1 = new ScannerConfiguration();
        sc1.fetchColumn(column.getFamily(), column.getQualifier());
        sc1.setSpan(Span.prefix(Bytes.of(nodeId)));

        return tx.get(sc1);
    }

    private boolean deleteDataBatch(Transaction tx, RowIterator iter, Column column) {
        try (TypedTransaction typeTx = new StringTypeLayer().wrap(tx)) {
            int count = 0;
            while (iter.hasNext() && count < batchSize) {
                Bytes row = iter.next().getKey();
                count++;
                tx.delete(row, column);
            }
            boolean hasNext = iter.hasNext();
            tx.commit();
            return hasNext;
        }
    }

    private String getQueryIdFromPcjId(Transaction tx, String pcjId) {
        Bytes queryIdBytes = tx.get(Bytes.of(pcjId), FluoQueryColumns.PCJ_ID_QUERY_ID);
        return queryIdBytes.toString();
    }

    private String getSparqlFromQueryId(Transaction tx, String queryId) {
        QueryMetadata metadata = dao.readQueryMetadata(tx, queryId);
        return metadata.getSparql();
    }

}
