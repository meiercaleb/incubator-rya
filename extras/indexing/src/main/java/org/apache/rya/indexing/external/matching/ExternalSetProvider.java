package org.apache.rya.indexing.external.matching;

import java.util.Iterator;
import java.util.List;

import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

/**
 * Interface for extracting {@link ExternalSet}s from specified {@link QuerySegment}s.
 *
 * @param <T> - Extension of ExternalSet class
 */
public interface ExternalSetProvider<T extends ExternalSet> {

    /**
     * Extract all {@link ExternalSet}s from specified QuerySegment.
     * 
     * @param segment
     * @return - List of ExternalSets
     */
    public List<T> getExternalSets(QuerySegment<T> segment);
    
    /**
     * Extract an Iterator over Lists of ExternalSets. This allows an ExtenalSetProvider to pass back
     * different combinations of ExternalSets for the purposes of query optimization.
     * 
     * @param segment
     * @return - Iterator over different combinations of ExternalSets
     */
    public Iterator<List<T>> getExternalSetCombos(QuerySegment<T> segment);
    
}
