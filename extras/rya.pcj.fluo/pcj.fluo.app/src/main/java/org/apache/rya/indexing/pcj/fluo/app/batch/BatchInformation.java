package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.data.Column;

public interface BatchInformation {

    public static enum Task {Add, Delete, Update}
    public static int DEFAULT_BATCH_SIZE = 5000;
    
    public int getBatchSize();
    
    public Task getTask();
    
    public Column getColumn();
    
    public BatchBindingSetUpdater getBatchUpdater();
    
}
