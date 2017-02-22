package org.apache.rya.indexing.pcj.fluo.app;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import java.util.HashSet;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicBinMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
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
public class PeriodicBinUpdater {

    private static final Logger log = Logger.getLogger(PeriodicBinUpdater.class);
    public static final String BIN_ID = "PeriodEndTime";
    private static final ValueFactory vf = new ValueFactoryImpl();
    private static final BindingSetStringConverter idConverter = new BindingSetStringConverter();
    private static final VisibilityBindingSetStringConverter valueConverter = new VisibilityBindingSetStringConverter();


    public void updatePeriodicBinResults(TransactionBase tx, VisibilityBindingSet bs, PeriodicBinMetadata metadata) {
        Set<Long> binIds = getBinEndTimes(metadata, bs);
        for(Long id: binIds) {
            QueryBindingSet binnedBs = new QueryBindingSet(bs);
            binnedBs.addBinding(BIN_ID, vf.createLiteral(id));
            String binnedBindingSetStringId = idConverter.convert(binnedBs, metadata.getVariableOrder());
            String binnedBindingSetStringValue = valueConverter.convert(binnedBs, metadata.getVariableOrder());
            String row = metadata.getNodeId() + NODEID_BS_DELIM + binnedBindingSetStringId;
            Column col = FluoQueryColumns.PERIODIC_BIN_BINDING_SET;
            String value = binnedBindingSetStringValue;
            tx.set(row, col, value);
        }
    }

    /**
     * This method returns the end times of all period windows containing the time contained in
     * the BindingSet.  
     * 
     * @param metadata
     * @return Set of period bin end times
     */
    private Set<Long> getBinEndTimes(PeriodicBinMetadata metadata, VisibilityBindingSet bs) {
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
    private long getFirstBinEndTime(long eventDateTime, long startTime, int periodTime) {
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
    private Set<Long> getEndTimes(long eventDateTime, long startTime, int windowSize, int periodTime) {
        Set<Long> binIds = new HashSet<>();
        long firstBinEndTime = getFirstBinEndTime(eventDateTime, startTime, periodTime);
        int numBins = 0;
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
