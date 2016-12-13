package org.apache.rya.indexing.external.matching;

import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

public interface ExternalSetConverter<T extends ExternalSet> {
    
    /**
     * Converts the {@link ExternalSet} to a {@link QuerySegment}
     * 
     * @param set - ExternalSet to be converted
     * @return QuerySegment derived from ExternalSet
     */
    public QuerySegment<T> setToSegment(T set);

}
