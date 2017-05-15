package org.apache.rya.indexing.pcj.fluo.app.query;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.StatementPattern.Scope;

public class PeriodicQueryNode extends UnaryTupleOperator {

    private TimeUnit unit;
    private long windowDuration;
    private long periodDuration;
    private String temporalVar;
    
    public PeriodicQueryNode(long window, long period, TimeUnit unit, String temporalVar, TupleExpr arg) {
        super(arg);
        this.windowDuration = window;
        this.periodDuration = period;
        this.unit = unit;
        this.temporalVar = temporalVar;
    }
    
    public String getTemporalVariable() {
        return temporalVar;
    }

    /**
     * @return window duration in millis
     */
    public long getWindowSize() {
        return windowDuration;
    }

    /**
     * @return period duration in millis
     */
    public long getPeriod() {
        return periodDuration;
    }

    /**
     * @return {@link TimeUnit} for window duration and period duration
     */
    public TimeUnit getUnit() {
        return unit;
    }
    
    @Override
    public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
        visitor.meetOther(this);
    }
    
    @Override
    public boolean equals(Object other) {
        if(this == other) {
            return true;
        }
        
        if (other instanceof PeriodicQueryNode) {
            if (super.equals(other)) {
                PeriodicQueryNode metadata = (PeriodicQueryNode) other;
                return new EqualsBuilder().append(windowDuration, metadata.windowDuration).append(periodDuration, metadata.periodDuration)
                        .append(unit, metadata.unit).append(temporalVar, metadata.temporalVar).isEquals();
            }
            return false;
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(arg, unit, windowDuration, periodDuration, temporalVar);
    }
    
    @Override
    public String getSignature() {
        StringBuilder sb = new StringBuilder();

        sb.append("PeriodicQueryNode(");
        sb.append("Var = " + temporalVar + ", ");
        sb.append("Window = " + windowDuration + " ms, ");
        sb.append("Period = " + periodDuration + " ms, ");
        sb.append("Time Unit = " + unit  + ")");
       

        return sb.toString();
    }
    
    @Override
    public PeriodicQueryNode clone() {
        PeriodicQueryNode clone = (PeriodicQueryNode)super.clone();
        clone.setArg(getArg().clone());
        clone.periodDuration = periodDuration;
        clone.windowDuration = windowDuration;
        clone.unit = unit;
        clone.temporalVar = temporalVar;
        return clone;
    }

}
