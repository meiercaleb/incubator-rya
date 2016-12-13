package org.apache.rya.indexing.external.matching;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;

public class MatcherUtilities {

    public static boolean segmentContainsLeftJoins(final TupleExpr tupleExpr) {
        if (tupleExpr instanceof Projection) {
            return segmentContainsLeftJoins(((Projection) tupleExpr).getArg());
        } else if (tupleExpr instanceof Join) {
            final Join join = (Join) tupleExpr;
            return segmentContainsLeftJoins(join.getRightArg())
                    || segmentContainsLeftJoins(join.getLeftArg());
        } else if (tupleExpr instanceof LeftJoin) {
            return true;
        } else if (tupleExpr instanceof Filter) {
            return segmentContainsLeftJoins(((Filter) tupleExpr).getArg());
        } else {
            return false;
        }
    }
    
}
