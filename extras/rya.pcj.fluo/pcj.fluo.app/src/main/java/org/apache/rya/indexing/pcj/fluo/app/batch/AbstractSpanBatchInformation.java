package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;

import jline.internal.Preconditions;

public abstract class AbstractSpanBatchInformation extends BasicBatchInformation {

    private Span span;

    public AbstractSpanBatchInformation(int batchSize, Task task, Column column, Span span) {
        super(batchSize, task, column);
        Preconditions.checkNotNull(span);
        this.span = span;
    }

    public AbstractSpanBatchInformation(Task task, Column column, Span span) {
        this(DEFAULT_BATCH_SIZE, task, column, span);
    }

    public Span getSpan() {
        return span;
    }

    public void setSpan(Span span) {
        this.span = span;
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("Span Batch Information {\n")
                .append("    Batch Size: " + super.getBatchSize() + "\n")
                .append("    Task: " + super.getTask() + "\n")
                .append("    Column: " + super.getColumn() + "\n")
                .append("}")
                .toString();
    }
    

}
