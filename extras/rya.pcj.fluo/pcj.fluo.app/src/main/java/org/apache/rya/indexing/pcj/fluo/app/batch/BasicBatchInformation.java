package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.data.Column;

import com.google.common.base.Preconditions;

public abstract class BasicBatchInformation implements BatchInformation {
    
    private int batchSize;
    private Task task;
    private Column column;
    
    public BasicBatchInformation(int batchSize, Task task, Column column ) {
        Preconditions.checkNotNull(task);
        Preconditions.checkNotNull(column);
        this.batchSize = batchSize;
        this.task = task;
        this.column = column;
    }
    
    public BasicBatchInformation(Task task) {
        Preconditions.checkNotNull(task);
        this.task = task;
        this.batchSize = DEFAULT_BATCH_SIZE;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Task getTask() {
        return task;
    }
    
    public Column getColumn() {
        return column;
    }
    
}
