package org.apache.rya.indexing.pcj.fluo.app;

import java.util.HashSet;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Preconditions;

/**
 * This class adds the appropriate BinId Binding to each BindingSet that it processes.  The BinIds
 * are used to determine which period a BindingSet (with a temporal Binding) falls into so that
 * a user can receive periodic updates for a registered query. 
 *
 */
public class PeriodicQueryUpdater {

    private static final Logger log = Logger.getLogger(PeriodicQueryUpdater.class);
    public static final String BIN_ID = "PeriodEndTime";
    private static final ValueFactory vf = new ValueFactoryImpl();
    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();


    public void updatePeriodicBinResults(TransactionBase tx, VisibilityBindingSet bs, PeriodicQueryMetadata metadata) throws Exception {
        Set<Long> binIds = getBinEndTimes(metadata, bs);
        for(Long id: binIds) {
            //create binding set value bytes
            QueryBindingSet binnedBs = new QueryBindingSet(bs);
            binnedBs.addBinding(BIN_ID, vf.createLiteral(id));
            VisibilityBindingSet visibilityBindingSet = new VisibilityBindingSet(binnedBs, bs.getVisibility());
            Bytes periodicBsBytes = BS_SERDE.serialize(visibilityBindingSet);
            
            //create row 
            final Bytes resultRow = RowKeyUtil.makeRowKey(metadata.getNodeId(), metadata.getVariableOrder(), binnedBs);
            Column col = FluoQueryColumns.PERIODIC_QUERY_BINDING_SET;
            tx.set(resultRow, col, periodicBsBytes);
        }
    }

    /**
     * This method returns the end times of all period windows containing the time contained in
     * the BindingSet.  
     * 
     * @param metadata
     * @return Set of period bin end times
     */
    private Set<Long> getBinEndTimes(PeriodicQueryMetadata metadata, VisibilityBindingSet bs) {
        Set<Long> binIds = new HashSet<>();
        try {
            String timeVar = metadata.getTemporalVariable();
            Value value = bs.getBinding(timeVar).getValue();
            Literal temporalLiteral = (Literal) value;
            long eventDateTime = temporalLiteral.calendarValue().toGregorianCalendar().getTimeInMillis();
            return getEndTimes(eventDateTime, metadata.getStartTime(), metadata.getWindowSize(), metadata.getPeriod());
        } catch (Exception e) {
            log.trace("Unable to extract the entity time from BindingSet: " + bs);
        }
        return binIds;
    }

    /**
     * This method returns the smallest period end time that is greater than the
     * eventDateTime.
     * 
     * @param eventDateTime
     * @param startTime - when the periodic notification was first registered
     * @param periodTime 
     * @return - smallest period end time greater than the eventDateTime
     */
    private long getFirstBinEndTime(long eventDateTime, long startTime, long periodTime) {
        Preconditions.checkArgument(startTime < eventDateTime);
        return ((eventDateTime - startTime) / periodTime + 1) * periodTime + startTime;
    }

    /**
     * Using the smallest period end time, this method also creates all other period end times
     * that occur within one windowSize of the eventDateTime.
     * @param eventDateTime
     * @param startTime
     * @param windowSize
     * @param periodTime
     * @return Set of period bin end times
     */
    private Set<Long> getEndTimes(long eventDateTime, long startTime, long windowSize, long periodTime) {
        Set<Long> binIds = new HashSet<>();
        long firstBinEndTime = getFirstBinEndTime(eventDateTime, startTime, periodTime);
        long numBins = 0;
        if (windowSize % periodTime == 0) {
            numBins = windowSize / periodTime - 1;
        } else {
            numBins = windowSize / periodTime;
        }

        for (int i = 0; i <= numBins; i++) {
            binIds.add(firstBinEndTime + i * periodTime);
        }
        return binIds;
    }
    

}
