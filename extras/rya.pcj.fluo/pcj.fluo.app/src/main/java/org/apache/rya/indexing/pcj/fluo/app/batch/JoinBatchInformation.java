package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import jline.internal.Preconditions;

public class JoinBatchInformation extends AbstractSpanBatchInformation {

    private static final BatchBindingSetUpdater updater = new JoinBatchBindingSetUpdater();
    private VisibilityBindingSet bs;
    private VariableOrder varOrder;
    private Side side;
    private JoinType join;
    
    public Side getSide() {
        return side;
    }

    public JoinType getJoinType() {
        return join;
    }

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
    
    public VariableOrder getVarOrder() {
        return varOrder;
    }

   public VisibilityBindingSet getBs() {
        return bs;
    }
    
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
   
        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
     
        public Builder setTask(Task task) {
            this.task = task;
            return this;
        }
        
        public Builder setColumn(Column column) {
            this.column = column;
            return this;
        }
        
        public Builder setSpan(Span span) {
            this.span = span;
            return this;
        }
      
        public Builder setBs(VisibilityBindingSet bs) {
            this.bs = bs;
            return this;
        }
        
        public Builder setJoinType(JoinType join) {
            this.join = join;
            return this;
        }
        public Builder setSide(Side side) {
            this.side = side;
            return this;
        }
   
        public Builder setVarOrder(VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }
        
        
        public JoinBatchInformation build() {
            return new JoinBatchInformation(batchSize, task, column, span, bs, varOrder, side, join); 
        }
        
    }
}
