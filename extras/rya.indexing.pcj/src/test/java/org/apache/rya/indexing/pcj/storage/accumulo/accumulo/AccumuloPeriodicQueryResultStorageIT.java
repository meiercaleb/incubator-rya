package org.apache.rya.indexing.pcj.storage.accumulo.accumulo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PeriodicQueryTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

public class AccumuloPeriodicQueryResultStorageIT extends AccumuloITBase {

    private PeriodicQueryResultStorage periodicStorage;
    private static final String RYA = "rya_";
    private static final PeriodicQueryTableNameFactory nameFactory = new PeriodicQueryTableNameFactory();
    private static final ValueFactory vf = new ValueFactoryImpl();
    
    @Before
    public void init() throws AccumuloException, AccumuloSecurityException {
        super.getConnector().securityOperations().changeUserAuthorizations("root", new Authorizations("U"));
        periodicStorage = new AccumuloPeriodicQueryResultStorage(super.getConnector(), RYA);
    }
    
    
    @Test
    public void testCreateAndMeta() throws PeriodicQueryStorageException {
        
        String sparql = "select ?x where { ?x <urn:pred> ?y.}";
        VariableOrder varOrder = new VariableOrder("periodicBinId", "x");
        PeriodicQueryStorageMetadata expectedMeta = new PeriodicQueryStorageMetadata(sparql, varOrder);
        
        String id = periodicStorage.createPeriodicQuery(sparql);
        Assert.assertEquals(expectedMeta, periodicStorage.getPeriodicQueryMetadata(id));
        Assert.assertEquals(Arrays.asList(nameFactory.makeTableName(RYA, id)), periodicStorage.listPeriodicTables());
        
    }
    
    
    @Test
    public void testAddListDelete() throws Exception {
        
        String sparql = "select ?x where { ?x <urn:pred> ?y.}";
        String id = periodicStorage.createPeriodicQuery(sparql);
        
        Set<BindingSet> expected = new HashSet<>();
        Set<VisibilityBindingSet> storageSet = new HashSet<>();
        
        //add result matching user's visibility
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("periodicBinId", vf.createLiteral(1L));
        bs.addBinding("x",vf.createURI("uri:uri123"));
        expected.add(bs);
        storageSet.add(new VisibilityBindingSet(bs,"U"));
        
        //add result with different visibility that is not expected
        bs = new QueryBindingSet();
        bs.addBinding("periodicBinId", vf.createLiteral(1L));
        bs.addBinding("x",vf.createURI("uri:uri456"));
        storageSet.add(new VisibilityBindingSet(bs,"V"));
        
        periodicStorage.addPeriodicQueryResults(id, storageSet);
        
        Set<BindingSet> actual = new HashSet<>();
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(id, Optional.of(1L))) {
            iter.forEachRemaining(x -> actual.add(x));
        }
        
        Assert.assertEquals(expected, actual);
        
        periodicStorage.deletePeriodicQueryResults(id, 1L);
        
        Set<BindingSet> actual2 = new HashSet<>();
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(id, Optional.of(1L))) {
            iter.forEachRemaining(x -> actual2.add(x));
        }
        
        Assert.assertEquals(new HashSet<>(), actual2);
        
    }
    
    
    
}
