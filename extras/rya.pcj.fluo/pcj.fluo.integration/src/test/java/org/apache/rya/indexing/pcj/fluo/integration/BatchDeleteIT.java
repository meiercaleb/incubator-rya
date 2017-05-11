package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.RyaExportITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.CreateBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class BatchDeleteIT extends RyaExportITBase {

    private static final Logger log = Logger.getLogger(BatchDeleteIT.class);
    private static final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

    @Test
    public void simpleScanDelete() throws Exception {

        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration());
        
        RyaURI subj = new RyaURI("urn:subject_1");
        RyaStatement statement1 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);
        RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);
        Set<RyaStatement> statements1 = getRyaStatements(statement1, 10);
        Set<RyaStatement> statements2 = getRyaStatements(statement2, 10);

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        String queryId = new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), RYA_INSTANCE_NAME);

        List<String> ids = getNodeIdStrings(queryId);
        List<String> prefixes = Arrays.asList("urn:subject_1", "urn:object", "urn:subject_1", "urn:subject_1");

        // Stream the data into Fluo.
        InsertTriples inserter = new InsertTriples();
        inserter.insert(fluoClient, statements1, Optional.<String> absent());
        inserter.insert(fluoClient, statements2, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        getMiniFluo().waitForObservers();
     

        verifyCounts(fluoClient, ids, Arrays.asList(100, 100, 10, 10));

        createSpanBatches(fluoClient, ids, prefixes, 10);
        getMiniFluo().waitForObservers();

        verifyCounts(fluoClient, ids, Arrays.asList(0, 0, 0, 0));
    }

    @Test
    public void simpleJoinDelete() throws Exception {
        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration());
        
        RyaURI subj = new RyaURI("urn:subject_1");
        RyaStatement statement1 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);
        RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);
        Set<RyaStatement> statements1 = getRyaStatements(statement1, 5);
        Set<RyaStatement> statements2 = getRyaStatements(statement2, 5);

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        String queryId = new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), RYA_INSTANCE_NAME);

        List<String> ids = getNodeIdStrings(queryId);
        String joinId = ids.get(1);
        String rightSp = ids.get(3);
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("subject", new URIImpl("urn:subject_1"));
        bs.addBinding("object1", new URIImpl("urn:object_0"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        Span span = Span.prefix(Bytes.of(rightSp + IncrementalUpdateConstants.NODEID_BS_DELIM + "urn:subject_1"));
        VariableOrder varOrder = new VariableOrder(Arrays.asList("subject", "object2"));

        // Stream the data into Fluo.
        InsertTriples inserter = new InsertTriples();
        inserter.insert(fluoClient, statements1, Optional.<String> absent());
        inserter.insert(fluoClient, statements2, Optional.<String> absent());

        getMiniFluo().waitForObservers();
        verifyCounts(fluoClient, ids, Arrays.asList(25, 25, 5, 5));
        
        JoinBatchInformation batch = JoinBatchInformation.builder().setBatchSize(1)
                .setColumn(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET).setSpan(span).setTask(Task.Delete)
                .setJoinType(JoinType.NATURAL_JOIN).setSide(Side.LEFT).setBs(vBs).setVarOrder(varOrder).build();
        // Verify the end results of the query match the expected results.
        createSpanBatch(fluoClient, joinId, batch);
        
        getMiniFluo().waitForObservers();
        verifyCounts(fluoClient, ids, Arrays.asList(25, 20, 5, 5));
    }

    
    @Test
    public void simpleJoinAdd() throws Exception {
        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration());

        RyaURI subj = new RyaURI("urn:subject_1");
        RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);
        Set<RyaStatement> statements2 = getRyaStatements(statement2, 5);

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        String queryId = new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), RYA_INSTANCE_NAME);

        List<String> ids = getNodeIdStrings(queryId);
        String joinId = ids.get(1);
        String rightSp = ids.get(3);
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("subject", new URIImpl("urn:subject_1"));
        bs.addBinding("object1", new URIImpl("urn:object_0"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        Span span = Span.prefix(Bytes.of(rightSp + IncrementalUpdateConstants.NODEID_BS_DELIM + "urn:subject_1"));
        VariableOrder varOrder = new VariableOrder(Arrays.asList("subject", "object2"));

        // Stream the data into Fluo.
        InsertTriples inserter = new InsertTriples();
        inserter.insert(fluoClient, statements2, Optional.<String> absent());

        getMiniFluo().waitForObservers();
        verifyCounts(fluoClient, ids, Arrays.asList(0, 0, 0, 5));
        
        JoinBatchInformation batch = JoinBatchInformation.builder().setBatchSize(1)
                .setColumn(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET).setSpan(span).setTask(Task.Add)
                .setJoinType(JoinType.NATURAL_JOIN).setSide(Side.LEFT).setBs(vBs).setVarOrder(varOrder).build();
        // Verify the end results of the query match the expected results.
        createSpanBatch(fluoClient, joinId, batch);
        
        getMiniFluo().waitForObservers();
        verifyCounts(fluoClient, ids, Arrays.asList(5, 5, 0, 5));
    }

    
    
    private Set<RyaStatement> getRyaStatements(RyaStatement statement, int numTriples) {

        Set<RyaStatement> statements = new HashSet<>();
        final String subject = "urn:subject_";
        final String predicate = "urn:predicate_";
        final String object = "urn:object_";

        for (int i = 0; i < numTriples; i++) {
            RyaStatement stmnt = new RyaStatement(statement.getSubject(), statement.getPredicate(), statement.getObject());
            if (stmnt.getSubject() == null) {
                stmnt.setSubject(new RyaURI(subject + i));
            }
            if (stmnt.getPredicate() == null) {
                stmnt.setPredicate(new RyaURI(predicate + i));
            }
            if (stmnt.getObject() == null) {
                stmnt.setObject(new RyaURI(object + i));
            }
            statements.add(stmnt);
        }
        return statements;
    }

    private List<String> getNodeIdStrings(String queryId) {
        List<String> nodeStrings = new ArrayList<>();
        FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration());
        try (Snapshot sx = fluoClient.newSnapshot()) {
            FluoQuery query = dao.readFluoQuery(sx, queryId);
            nodeStrings.add(queryId);
            Collection<JoinMetadata> jMeta = query.getJoinMetadata();
            for (JoinMetadata meta : jMeta) {
                nodeStrings.add(meta.getNodeId());
                nodeStrings.add(meta.getLeftChildNodeId());
                nodeStrings.add(meta.getRightChildNodeId());
            }
        }
        return nodeStrings;
    }

    private void createSpanBatches(FluoClient fluoClient, List<String> ids, List<String> prefixes, int batchSize) {

        Preconditions.checkArgument(ids.size() == prefixes.size());
        
        try (Transaction tx = fluoClient.newTransaction()) {
            for (int i = 0; i < ids.size(); i++) {
                String id = ids.get(i);
                String bsPrefix = prefixes.get(i);
                NodeType type = NodeType.fromNodeId(id).get();
                Column bsCol = type.getBsColumn();
                String row = id + IncrementalUpdateConstants.NODEID_BS_DELIM + bsPrefix;
                Span span = Span.prefix(Bytes.of(row));
                BatchInformation batch = SpanBatchDeleteInformation.builder().setBatchSize(batchSize).setColumn(bsCol).setSpan(span)
                        .build();
                CreateBatchInformation.createBatch(tx, id, batch);
            }
            tx.commit();
        }
    }
    
    private void createSpanBatch(FluoClient fluoClient, String nodeId, BatchInformation batch) {
        try(Transaction tx = fluoClient.newTransaction()) {
            CreateBatchInformation.createBatch(tx, nodeId, batch);
            tx.commit();
        }
    }

    private int countResults(FluoClient fluoClient, String nodeId, Column bsColumn) {
        try (Transaction tx = fluoClient.newTransaction()) {
            int count = 0;
            RowScanner scanner = tx.scanner().over(Span.prefix(nodeId)).fetch(bsColumn).byRow().build();
            Iterator<ColumnScanner> colScanners = scanner.iterator();
            while (colScanners.hasNext()) {
                ColumnScanner colScanner = colScanners.next();
                Iterator<ColumnValue> vals = colScanner.iterator();
                while (vals.hasNext()) {
                    vals.next();
                    count++;
                }
            }
            tx.commit();
            return count;
        }
    }

    private void verifyCounts(FluoClient fluoClient, List<String> ids, List<Integer> expectedCounts) {
        Preconditions.checkArgument(ids.size() == expectedCounts.size());
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            int expected = expectedCounts.get(i);
            NodeType type = NodeType.fromNodeId(id).get();
            int count = countResults(fluoClient, id, type.getBsColumn());
            log.trace("NodeId: " + id + " Count: " + count + " Expected: " + expected);
            switch (type) {
            case STATEMENT_PATTERN:
                assertEquals(expected, count);
                break;
            case JOIN:
                assertEquals(expected, count);
                break;
            case QUERY:
                assertEquals(expected, count);
                break;
            default:
                break;
            }
        }
    }

}
