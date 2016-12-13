package org.apache.rya.indexing.pcj.matching;

import org.apache.rya.indexing.external.matching.ExternalSetConverter;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.OptionalJoinSegment;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link ExternalSetConverter} to convert {@link ExternalTupleSet}s
 * to {@link QuerySegment}s. 
 *
 */
public class PCJToSegmentConverter implements ExternalSetConverter<ExternalTupleSet> {

    private static final PCJToOptionalJoinSegment optional = new PCJToOptionalJoinSegment();
    private static final PCJToJoinSegment join = new PCJToJoinSegment();
    
    
    @Override
    public QuerySegment<ExternalTupleSet> setToSegment(ExternalTupleSet set) {
        Preconditions.checkNotNull(set);
        if (PCJOptimizerUtilities.tupleContainsLeftJoins(set.getTupleExpr())) {
            return optional.getSegment(set);
        } else {
            return join.getSegment(set);
        }
    }

    /**
     * This class extracts the {@link JoinSegment} from the {@link TupleExpr} of
     * specified PCJ.
     *
     */
    static class PCJToJoinSegment extends QueryModelVisitorBase<RuntimeException> {

        private JoinSegment<ExternalTupleSet> segment;
        
        private PCJToJoinSegment(){};

        public QuerySegment<ExternalTupleSet> getSegment(ExternalTupleSet pcj) {
            segment = null;
            pcj.getTupleExpr().visit(this);
            return segment;
        }

        @Override
        public void meet(final Join join) {
            segment = new JoinSegment<ExternalTupleSet>(join);
        }

        @Override
        public void meet(final Filter filter) {
            segment = new JoinSegment<ExternalTupleSet>(filter);
        }

    }

    /**
     * This class extracts the {@link OptionalJoinSegment} of PCJ query.
     *
     */
    static class PCJToOptionalJoinSegment extends QueryModelVisitorBase<RuntimeException> {

        private OptionalJoinSegment<ExternalTupleSet> segment;
        
        private PCJToOptionalJoinSegment(){};

        public QuerySegment<ExternalTupleSet> getSegment(ExternalTupleSet pcj) {
            segment = null;
            pcj.getTupleExpr().visit(this);
            return segment;
        }

        @Override
        public void meet(final Join join) {
            segment = new OptionalJoinSegment<ExternalTupleSet>(join);
        }

        @Override
        public void meet(final Filter filter) {
            segment = new OptionalJoinSegment<ExternalTupleSet>(filter);
        }

        @Override
        public void meet(final LeftJoin node) {
            segment = new OptionalJoinSegment<ExternalTupleSet>(node);
        }

    }

}
