package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.storage.PCJIdFactory;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.AggregateOperatorBase;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Preconditions;

import info.aduna.iteration.CloseableIteration;

public class AccumuloPeriodicQueryResultStorage implements PeriodicQueryResultStorage {

    private String ryaInstance;
    private Connector accumuloConn;
    private Authorizations auths;
    private final PCJIdFactory pcjIdFactory = new PCJIdFactory();
    private final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
    private static final PcjTables pcjTables = new PcjTables();
    private static final PeriodicQueryTableNameFactory tableNameFactory = new PeriodicQueryTableNameFactory();

    public AccumuloPeriodicQueryResultStorage(Connector accumuloConn, String ryaInstance) {
        this.accumuloConn = accumuloConn;
        this.ryaInstance = ryaInstance;
        String user = accumuloConn.whoami();
        try {
            this.auths = accumuloConn.securityOperations().getUserAuthorizations(user);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Unable access user: " + user + "authorizations.");
        }
    }

    @Override
    public String createPeriodicQuery(String sparql) throws PeriodicQueryStorageException {
        Preconditions.checkNotNull(sparql);
        String queryId = pcjIdFactory.nextId();
        return createPeriodicQuery(queryId, sparql);
    }
    
    @Override
    public String createPeriodicQuery(String queryId, String sparql) throws PeriodicQueryStorageException {
        Set<String> bindingNames;
        try {
            bindingNames = new AggregateVariableRemover().getNonAggregationVariables(sparql);
        } catch (MalformedQueryException e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
        List<String> varOrderList = new ArrayList<>();
        varOrderList.add(PeriodicQueryResultStorage.PeriodicBinId);
        varOrderList.addAll(bindingNames);
        createPeriodicQuery(queryId, sparql, new VariableOrder(varOrderList));
        return queryId;
    }

    @Override
    public void createPeriodicQuery(String queryId, String sparql, VariableOrder order) throws PeriodicQueryStorageException {
        Preconditions.checkNotNull(sparql);
        Preconditions.checkNotNull(queryId);
        Preconditions.checkNotNull(order);
        Preconditions.checkArgument(PeriodicQueryResultStorage.PeriodicBinId.equals(order.getVariableOrders().get(0)),
                "periodicBinId binding name must occur first in VariableOrder.");
        String tableName = tableNameFactory.makeTableName(ryaInstance, queryId);
        Set<VariableOrder> varOrders = new HashSet<>();
        varOrders.add(order);
        try {
            pcjTables.createPcjTable(accumuloConn, tableName, varOrders, sparql);
        } catch (Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public PeriodicQueryStorageMetadata getPeriodicQueryMetadata(String queryId) throws PeriodicQueryStorageException {
        try {
            return new PeriodicQueryStorageMetadata(
                    pcjTables.getPcjMetadata(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId)));
        } catch (Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public void addPeriodicQueryResults(String queryId, Collection<VisibilityBindingSet> results) throws PeriodicQueryStorageException {
        results.forEach(x -> Preconditions.checkArgument(x.hasBinding(PeriodicQueryResultStorage.PeriodicBinId),
                "BindingSet must contain periodBinId binding."));
        try {
            pcjTables.addResults(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId), results);
        } catch (Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public void deletePeriodicQueryResults(String queryId, long binId) throws PeriodicQueryStorageException {
        String tableName = tableNameFactory.makeTableName(ryaInstance, queryId);
        try {
            Text prefix = getRowPrefix(binId);
            accumuloConn.tableOperations().deleteRows(tableName, prefix, Range.followingPrefix(prefix));
        } catch (Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    public void deletePeriodicQueryResults(String queryId) throws PeriodicQueryStorageException {
        try {
            pcjTables.purgePcjTable(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId));
        } catch (Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public void deletePeriodicQuery(String queryId) throws PeriodicQueryStorageException {
        try {
            pcjTables.dropPcjTable(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId));
        } catch (Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public CloseableIterator<BindingSet> listResults(String queryId, Optional<Long> binId)
            throws PeriodicQueryStorageException {
        requireNonNull(queryId);

        String tableName = tableNameFactory.makeTableName(ryaInstance, queryId);
        // Fetch the Variable Orders for the binding sets and choose one of
        // them. It
        // doesn't matter which one we choose because they all result in the
        // same output.
        final PeriodicQueryStorageMetadata metadata = getPeriodicQueryMetadata(queryId);
        final VariableOrder varOrder = metadata.getVariableOrder();

        try {
            // Fetch only the Binding Sets whose Variable Order matches the
            // selected one.
            final Scanner scanner = accumuloConn.createScanner(tableName, auths);
            scanner.fetchColumnFamily(new Text(varOrder.toString()));
            if (binId.isPresent()) {
                scanner.setRange(Range.prefix(getRowPrefix(binId.get())));
            }
            return new AccumuloValueBindingSetIterator(scanner);

        } catch (Exception e) {
            throw new PeriodicQueryStorageException(String.format("PCJ Table does not exist for name '%s'.", tableName), e);
        }
    }
    
    private Text getRowPrefix(long binId) throws BindingSetConversionException {
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding(PeriodicQueryResultStorage.PeriodicBinId, new LiteralImpl(Long.toString(binId), XMLSchema.LONG));
        
        return new Text(converter.convert(bs, new VariableOrder(PeriodicQueryResultStorage.PeriodicBinId)));
    }

    @Override
    public List<String> listPeriodicTables() {

        final List<String> periodicTables = new ArrayList<>();
        final String periodicPrefix = ryaInstance + PeriodicQueryTableNameFactory.PeriodicTableSuffix;
        boolean foundInstance = false;

        for (final String tableName : accumuloConn.tableOperations().list()) {
            if (tableName.startsWith(ryaInstance)) {
                // This table is part of the target Rya instance.
                foundInstance = true;

                if (tableName.startsWith(periodicPrefix)) {
                    periodicTables.add(tableName);
                }
            } else if (foundInstance) {
                // We have encountered the first table name that does not start
                // with the rya instance name after those that do. Because the
                // list is sorted, there can't be any more pcj tables for the
                // target instance in the list.
                break;
            }
        }
        return periodicTables;
    }
    
    static class AggregateVariableRemover extends QueryModelVisitorBase<RuntimeException> {
        
        private Set<String> bindingNames;
        
        public Set<String> getNonAggregationVariables(String sparql) throws MalformedQueryException {
            TupleExpr te = new SPARQLParser().parseQuery(sparql, null).getTupleExpr();
            bindingNames = te.getBindingNames();
            te.visit(this);
            return bindingNames;
        }
        
        @Override
        public void meet(ExtensionElem node) {
            if(node.getExpr() instanceof AggregateOperatorBase) {
                bindingNames.remove(node.getName());
            }
        }
        
    }

}
