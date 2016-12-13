package org.apache.rya.indexing.external.matching;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.base.Preconditions;

/**
 * Factory class for producing {@link QuerySegment}s from {@link QueryModelNode}s.
 *
 * @param <T> - ExternalSet parameter
 */
public class QuerySegmentFactory<T extends ExternalSet> {

    public QuerySegment<T> getQuerySegment(QueryModelNode node) {
        Preconditions.checkNotNull(node);
        if(node instanceof Filter) {
            Filter filter = (Filter)node;
            if(MatcherUtilities.segmentContainsLeftJoins(filter)) {
                return new OptionalJoinSegment<T>(filter);
            } else {
                return new JoinSegment<T>(filter);
            }
        } else if(node instanceof Join) {
            Join join = (Join) node;
            if(MatcherUtilities.segmentContainsLeftJoins(join)) {
                return new OptionalJoinSegment<T>(join);
            } else {
                return new JoinSegment<T>(join);
            }
        } else if (node instanceof LeftJoin) {
            return new OptionalJoinSegment<T>((LeftJoin) node);
        } else {
            throw new IllegalArgumentException("Node must be a Join, Filter, or LeftJoin");
        }
        
    }
    
}
