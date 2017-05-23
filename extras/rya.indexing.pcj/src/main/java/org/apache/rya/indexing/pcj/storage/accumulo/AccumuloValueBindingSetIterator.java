package org.apache.rya.indexing.pcj.storage.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.openrdf.query.BindingSet;

import info.aduna.iteration.CloseableIteration;

public class AccumuloValueBindingSetIterator implements CloseableIteration<BindingSet,Exception>{
    
    private final Scanner scanner;
    private final Iterator<Entry<Key, Value>> iter;
    private final VisibilityBindingSetSerDe bsSerDe = new VisibilityBindingSetSerDe();
    
    public AccumuloValueBindingSetIterator(Scanner scanner) {
        this.scanner = scanner;
        iter = scanner.iterator();
    }
    
    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }
    
    @Override 
    public BindingSet next() throws PeriodicQueryStorageException {
        try {
            return bsSerDe.deserialize(Bytes.of(iter.next().getValue().get())).set;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public void remove() throws Exception {
        throw new UnsupportedOperationException();
    }
    

}
