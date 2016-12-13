package org.apache.rya.indexing.pcj.matching;

import org.apache.rya.indexing.external.matching.AbstractExternalSetMatcherFactory;
import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.JoinSegmentMatcher;
import org.apache.rya.indexing.external.matching.OptionalJoinSegment;
import org.apache.rya.indexing.external.matching.OptionalJoinSegmentMatcher;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

/**
 * Factory used to build {@link ExternalSetMatcher}s for the {@link PCJOptimizer}.
 *
 */
public class PCJExternalSetMatcherFactory extends AbstractExternalSetMatcherFactory<ExternalTupleSet> {

    @Override
    protected ExternalSetMatcher<ExternalTupleSet> getJoinSegmentMatcher(JoinSegment<ExternalTupleSet> segment) {
        return new JoinSegmentMatcher<ExternalTupleSet>(segment, new PCJToSegmentConverter());
    }

    @Override
    protected ExternalSetMatcher<ExternalTupleSet> getOptionalJoinSegmentMatcher(OptionalJoinSegment<ExternalTupleSet> segment) {
        return new OptionalJoinSegmentMatcher<ExternalTupleSet>(segment, new PCJToSegmentConverter());
    }

}
