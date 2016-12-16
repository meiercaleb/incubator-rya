package org.apache.rya.indexing.pcj.matching;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.matching.AbstractExternalSetOptimizer;
import org.apache.rya.indexing.external.matching.BasicRater;
import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QueryNodeListRater;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.matching.TopOfQueryFilterRelocator;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;

import com.google.common.base.Optional;;


/**
 * {@link QueryOptimizer} which matches {@link TupleExpr}s associated with
 * pre-computed queries to sub-queries of a given query. Each matched sub-query
 * is replaced by an {@link ExternalTupleSet} node to delegate that portion of
 * the query to the pre-computed query index.
 * <p>
 *
 * A query is be broken up into {@link QuerySegment}s. Pre-computed query
 * indices, or {@link ExternalTupleset} objects, are compared against the
 * {@link QueryModelNode}s in each QuerySegment. If an ExternalTupleSets nodes
 * match a subset of the given QuerySegments nodes, those nodes are replaced by
 * the ExternalTupleSet in the QuerySegment.
 *
 */
public class PCJOptimizer extends AbstractExternalSetOptimizer<ExternalTupleSet>implements Configurable {
    private static final PCJExternalSetMatcherFactory factory = new PCJExternalSetMatcherFactory();
    private AccumuloIndexSetProvider provider;
    private Configuration conf;
    private boolean init = false;

    public PCJOptimizer() {}

    public PCJOptimizer(final Configuration conf) {
       setConf(conf);
    }

    /**
     * This constructor is designed to be used for testing.  A more typical use
     * pattern is for a user to specify Accumulo connection details in a Configuration
     * file so that PCJs can be retrieved by an AccumuloIndexSetProvider.
     * 
     * @param indices - user specified PCJs to match to query
     * @param useOptimalPcj - optimize PCJ combos for matching
     */
    public PCJOptimizer(final List<ExternalTupleSet> indices, final boolean useOptimalPcj) {
        checkNotNull(indices);
        conf = new Configuration();
        this.useOptimal = useOptimalPcj;
        provider = new AccumuloIndexSetProvider(conf, indices);
        init = true;
    }

    @Override
    public final void setConf(final Configuration conf) {
        checkNotNull(conf);
        if (!init) {
            try {
                this.conf = conf;
                this.useOptimal = ConfigUtils.getUseOptimalPCJ(conf);
                provider = new AccumuloIndexSetProvider(conf);
            } catch (Exception e) {
                throw new Error(e);
            }
            init = true;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * This method optimizes a specified query by matching subsets of it with
     * PCJ queries.
     *
     * @param tupleExpr - the query to be optimized
     * @param dataset - this value is ignored
     * @param bindings - this value is ignored
     */
    @Override
    public void optimize(TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings) {
        checkNotNull(tupleExpr);
        // first standardize query by pulling all filters to top of query if
        // they exist using TopOfQueryFilterRelocator
        tupleExpr = TopOfQueryFilterRelocator.moveFiltersToTop(tupleExpr);
        try {
            if (provider.size() > 0) {
                super.optimize(tupleExpr, null, null);
            } else {
                return;
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not populate Accumulo Index Cache.");
        }
    }


    @Override
    protected ExternalSetMatcher<ExternalTupleSet> getMatcher(QuerySegment<ExternalTupleSet> segment) {
        return factory.getMatcher(segment);
    }

    @Override
    protected ExternalSetProvider<ExternalTupleSet> getProvider(QuerySegment<ExternalTupleSet> segment) {
        return provider;
    }

    @Override
    protected Optional<QueryNodeListRater> getNodeListRater(QuerySegment<ExternalTupleSet> segment) {
        return Optional.of(new BasicRater(segment.getOrderedNodes()));
    }
}
