package org.apache.rya.indexing.pcj.fluo.app.batch;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import java.util.Objects;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.Binding;

import jline.internal.Preconditions;

/**
 * This class updates join results based on parameters specified for the join's
 * children. The join has two children, and for one child a VisibilityBindingSet
 * is specified along with the Side of that child. This BindingSet represents an
 * update to that join child. For the other child, a Span, Column and
 * VariableOrder are specified. This is so that the sibling node (the node that
 * wasn't updated) can be scanned to obtain results that can be joined with the
 * VisibilityBindingSet. The assumption here is that the Span is derived from
 * the {@link Binding}s of common variables between the join children, with
 * Values ordered according to the indicated {@link VariableOrder}. This class
 * represents a batch order to perform a given task on join BindingSet results.
 * The {@link Task} is to Add, Delete, or Update. This batch order is processed
 * by the {@link BatchObserver} and used with the nodeId provided to the
 * Observer to process the Task specified by the batch order. If the Task is to
 * add, the BatchBindingSetUpdater returned by
 * {@link JoinBatchInformation#getBatchUpdater()} will scan the join's child for
 * results using the indicated Span and Column. These results are joined with
 * the indicated VisibilityBindingSet, and the results are added to the parent
 * join. The other Tasks are performed analogously.
 *
 */
public class JoinBatchInformation extends AbstractSpanBatchInformation {

    private static final BatchBindingSetUpdater updater = new JoinBatchBindingSetUpdater();
    private VisibilityBindingSet bs; //update for join child indicated by side
    private VariableOrder varOrder; //variable order for child indicated by Span
    private Side side;  //join child that was updated by bs
    private JoinType join;
    /**
     * @param batchSize - batch size that Tasks are performed in
     * @param task - Add, Delete, or Update
     * @param column - Column of join child to be scanned
     * @param span - span of join child to be scanned (derived from common variables of left and right join children)
     * @param bs - BindingSet to be joined with results of child scan
     * @param varOrder - VariableOrder used to form join (order for join child corresponding to Span)
     * @param side - The side of the child that the VisibilityBindingSet update occurred at
     * @param join - JoinType (left, right, natural inner)
     */
    public JoinBatchInformation(int batchSize, Task task, Column column, Span span, VisibilityBindingSet bs, VariableOrder varOrder, Side side, JoinType join) {
        super(batchSize, task, column, span);
        Preconditions.checkNotNull(bs);
        Preconditions.checkNotNull(varOrder);
        Preconditions.checkNotNull(side);
        Preconditions.checkNotNull(join);
        this.bs = bs;
        this.varOrder = varOrder;
        this.join = join;
        this.side = side;
    }
    
    public JoinBatchInformation(Task task, Column column, Span span, VisibilityBindingSet bs, VariableOrder varOrder, Side side, JoinType join) {
        this(DEFAULT_BATCH_SIZE, task, column, span, bs, varOrder, side, join);
    }
    
    /**
     * Indicates the join child that the BindingSet result {@link JoinBatchInformation#getBs()} updated.
     * This BindingSet is join with the results obtained by scanning over the value of {@link JoinBatchInformation#getSpan()}.
     * @return {@link Side} indicating which side new result occurred on in join
     */
    public Side getSide() {
        return side;
    }
    
    /**
     * @return {@link JoinType} indicating type of join (left join, right join, natural inner join,...)
     */
    public JoinType getJoinType() {
        return join;
    }
    
    /**
     * Returns the VariableOrder for the join child corresponding to the Span.
     * @return {@link VariableOrder} used to join {@link VisibilityBindingSet}s.
     */
    public VariableOrder getVarOrder() {
        return varOrder;
    }

   /**
    * Sets the VisibilityBindingSet that represents an update to the join child.  The join child
    * updated is indicated by the value of {@link JoinBatchInformation#getSide()}.
    * @return VisibilityBindingSet that will be joined with results returned by scan over given
    * {@link Span}.
    */
   public VisibilityBindingSet getBs() {
        return bs;
    }
    
   /**
    * @return BatchBindingSetUpdater used to apply {@link Task} to results formed by joining the given
    * VisibilityBindingSet with the results returned by scanned over the Span.
    */
    @Override
    public BatchBindingSetUpdater getBatchUpdater() {
        return updater;
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("Span Batch Information {\n")
                .append("    Batch Size: " + super.getBatchSize() + "\n")
                .append("    Task: " + super.getTask() + "\n")
                .append("    Column: " + super.getColumn() + "\n")
                .append("    VariableOrder: " + varOrder + "\n")
                .append("    Join Type: " + join + "\n")
                .append("    Join Side: " + side + "\n")
                .append("    Binding Set: " + bs + "\n")
                .append("}")
                .toString();
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof JoinBatchInformation)) {
            return false;
        }

        JoinBatchInformation batch = (JoinBatchInformation) other;
        return super.equals(other) &&  Objects.equals(this.bs, batch.bs) && Objects.equals(this.join, batch.join)
                && Objects.equals(this.side, batch.side) && Objects.equals(this.varOrder, batch.varOrder);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + super.hashCode();
        result = 31 * result + Objects.hashCode(bs);
        result = 31 * result + Objects.hashCode(join);
        result = 31 * result + Objects.hashCode(side);
        result = 31 * result + Objects.hashCode(varOrder);
        return result;
    }
    
    
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private int batchSize = DEFAULT_BATCH_SIZE;
        private Task task;
        private Column column;
        private Span span;
        private VisibilityBindingSet bs;
        private VariableOrder varOrder;
        private JoinType join;
        private Side side;
   
        /**
         * @param batchSize - batch size that {@link Task}s are performed in
         */
        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
     
        /**
         * @param task - Task performed (Add, Delete, Update)
         */
        public Builder setTask(Task task) {
            this.task = task;
            return this;
        }
        
        /**
         * @param column - Column of join child to be scanned
         */
        public Builder setColumn(Column column) {
            this.column = column;
            return this;
        }
        
        /**
         * Span to scan results for one child of the join. The Span corresponds to the side of 
         * the join that is not indicated by Side.  So if Side is Left, then the
         * Span will scan the right child of the join.  It is assumed that the span is derived from
         * the common variables of the left and right join children.
         * @param span - Span over join child to be scanned
         */
        public Builder setSpan(Span span) {
            this.span = span;
            return this;
        }
      
        /**
         * Sets the BindingSet that corresponds to an update to the join child indicated
         * by Side.  
         * @param bs - BindingSet update of join child to be joined with results of scan
         */
        public Builder setBs(VisibilityBindingSet bs) {
            this.bs = bs;
            return this;
        }
        
        /**
         * @param join - JoinType (left, right, natural inner)
         */
        public Builder setJoinType(JoinType join) {
            this.join = join;
            return this;
        }
        
        /**
         * Indicates the join child corresponding to the VisibilityBindingSet update
         * @param side - side of join the child BindingSet update appeared at
         */
        public Builder setSide(Side side) {
            this.side = side;
            return this;
        }
   
        /**
         * Sets the variable order for the join child corresponding to the Span
         * @param varOrder - Variable order used to join BindingSet with result of scan
         */
        public Builder setVarOrder(VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }
        
        public JoinBatchInformation build() {
            return new JoinBatchInformation(batchSize, task, column, span, bs, varOrder, side, join); 
        }
    }
}
