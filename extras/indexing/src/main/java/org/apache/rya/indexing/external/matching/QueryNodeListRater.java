package org.apache.rya.indexing.external.matching;

import java.util.List;

import org.openrdf.query.algebra.QueryModelNode;

/**
 * Class used for determining an optimal query plan.  It assigns a score
 * between 0 and 1 to a list of QueryModelNodes.  A lower score indicates 
 * that the List represents a better collection of nodes for building a query
 * plan.  Usually, the specified List is compared to a base List (original query),
 * and the specified List (mutated query) is compared to the original to determine
 * if there was any improvement in the query plan.  This is meant to be used in conjunction
 * with the method {@link ExternalSetMatcher#match(java.util.Iterator, com.google.common.base.Optional)}.
 *
 */
public interface QueryNodeListRater {
    
    public double rateQuerySegment(List<QueryModelNode> eNodes);
    
}
