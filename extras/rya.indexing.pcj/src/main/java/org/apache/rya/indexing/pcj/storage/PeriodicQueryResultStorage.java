package org.apache.rya.indexing.pcj.storage;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

import info.aduna.iteration.CloseableIteration;

public interface PeriodicQueryResultStorage {
    
    public static String PeriodicBinId = "periodicBinId";

    public String createPeriodicQuery(String sparql) throws PeriodicQueryStorageException;
    
    public String createPeriodicQuery(String queryId, String sparql) throws PeriodicQueryStorageException;
    
    public void createPeriodicQuery(String queryId, String sparql, VariableOrder varOrder) throws PeriodicQueryStorageException;
    
    public PeriodicQueryStorageMetadata getPeriodicQueryMetadata(String queryID) throws PeriodicQueryStorageException;;
    
    public void addPeriodicQueryResults(String queryId, Collection<VisibilityBindingSet> results) throws PeriodicQueryStorageException;;
    
    public void deletePeriodicQueryResults(String queryId, long binID) throws PeriodicQueryStorageException;;
    
    public void deletePeriodicQuery(String queryID) throws PeriodicQueryStorageException;;
    
    public CloseableIterator<BindingSet> listResults(String queryId, Optional<Long> binID) throws PeriodicQueryStorageException;;
    
    public List<String> listPeriodicTables();
    
}
