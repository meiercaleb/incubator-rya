package org.apache.rya.indexing.pcj.fluo.app.query;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Metadata that is required for periodic queries in the Rya Fluo Application.  
 * If a periodic query is registered with the Rya Fluo application, the BindingSets
 * are placed into temporal bins according to whether they occur within the window of
 * a period's ending time.  This Metadata is used to create a Bin Id, which is equivalent
 * to the period's ending time, to be inserted into each BindingSet that occurs within that
 * bin.  This is to allow the AggregationUpdater to aggregate the bins by grouping on the 
 * Bin Id.
 * 
 */
public class PeriodicQueryMetadata extends CommonNodeMetadata {

    private String parentNodeId;
    private String childNodeId;
    private int windowSize;
    private int period;
    private TimeUnit unit;
    private long startTime;
    private String temporalVariable;

    public PeriodicQueryMetadata(String nodeId, VariableOrder varOrder, String parentNodeId, String childNodeId, int windowSize, int period,
            TimeUnit unit, long startTime, String temporalVariable) {
        super(nodeId, varOrder);
        Preconditions.checkNotNull(parentNodeId);
        Preconditions.checkNotNull(childNodeId);
        Preconditions.checkNotNull(temporalVariable);
        Preconditions.checkNotNull(unit);
        Preconditions.checkNotNull(period > 0);
        Preconditions.checkArgument(windowSize >= period);
        Preconditions.checkArgument(startTime > 0);

        this.parentNodeId = parentNodeId;
        this.childNodeId = childNodeId;
        this.temporalVariable = temporalVariable;
        this.windowSize = windowSize;
        this.period = period;
        this.unit = unit;
        this.startTime = startTime;
    }

    public String getParentNodeId() {
        return parentNodeId;
    }

    public String getChildNodeId() {
        return childNodeId;
    }
    
    public String getTemporalVariable() {
        return temporalVariable;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public int getPeriod() {
        return period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public long getStartTime() {
        return startTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.getNodeId(), super.getVariableOrder(), childNodeId, parentNodeId, temporalVariable, startTime, period, windowSize, unit);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof PeriodicQueryMetadata) {
            if (super.equals(o)) {
                PeriodicQueryMetadata metadata = (PeriodicQueryMetadata) o;
                return new EqualsBuilder().append(childNodeId, metadata.childNodeId).append(parentNodeId, metadata.parentNodeId)
                        .append(startTime, metadata.startTime).append(windowSize, metadata.windowSize).append(period, metadata.period)
                        .append(unit, metadata.unit).append(temporalVariable, metadata.temporalVariable).isEquals();
            }
            return false;
        }

        return false;
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("PeriodicBinMetadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("    Child Node ID: " + childNodeId + "\n")
                .append("    Start Time: " + startTime + "\n")
                .append("    Period: " + period + "\n")
                .append("    Window Size: " + windowSize + "\n")
                .append("    Time Unit: " + unit + "\n")
                .append("    Temporal Variable: " + temporalVariable + "\n")
                .append("}")
                .toString();
    }


    public static class Builder {

        private String nodeId;
        private VariableOrder varOrder;
        private String parentNodeId;
        private String childNodeId;
        private int windowSize;
        private int period;
        private TimeUnit unit;
        private long startTime;
        public String temporalVariable;

        public Builder setNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }
        
        public Builder setVarOrder(VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }
        
        public Builder setParentNodeId(String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }

        public Builder setChildNodeId(String childNodeId) {
            this.childNodeId = childNodeId;
            return this;
        }

        public Builder setWindowSize(int windowSize) {
            this.windowSize = windowSize;
            return this;
        }

        public Builder setPeriod(int period) {
            this.period = period;
            return this;
        }

        public Builder setUnit(TimeUnit unit) {
            this.unit = unit;
            return this;
        }

        public Builder setStartTime(long startTime) {
            this.startTime = startTime;
            return this;
        }
        
        public Builder setTemporalVariable(String temporalVariable) {
            this.temporalVariable = temporalVariable;
            return this;
        }

        public PeriodicQueryMetadata build() {
            return new PeriodicQueryMetadata(nodeId, varOrder, parentNodeId, childNodeId, windowSize, period, unit, startTime, temporalVariable);
        }
    }

}
