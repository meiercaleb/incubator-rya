package org.apache.rya.indexing.external.matching;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

/**
 * This class takes in a given {@link Join}, {@Filter}, or {@link LeftJoin}
 * and provides the appropriate {@link ExternalSetMatcher} to match Entities to the
 * given query.
 */
public abstract class AbstractExternalSetMatcherFactory<T extends ExternalSet> {
    
    public ExternalSetMatcher<T> getMatcher(final QuerySegment<T> segment) {
        if(segment instanceof JoinSegment<?>) {
            return getJoinSegmentMatcher((JoinSegment<T>) segment);
        } else if(segment instanceof OptionalJoinSegment<?>) {
            return getOptionalJoinSegmentMatcher((OptionalJoinSegment<T>)segment);
        } else {
            throw new IllegalArgumentException("Invalid Segment.");
        }
    }
    
    protected abstract ExternalSetMatcher<T> getJoinSegmentMatcher(JoinSegment<T> segment);
    
    protected abstract ExternalSetMatcher<T> getOptionalJoinSegmentMatcher(OptionalJoinSegment<T> segment);

}
