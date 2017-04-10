package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

public class NoOpBatchInformation extends BasicBatchInformation {

    public NoOpBatchInformation() {
        super(0, Task.Add, FluoQueryColumns.TRIPLES);
    }

    private static final NoOpBatchUpdater updater = new NoOpBatchUpdater();
    
    @Override
    public BatchBindingSetUpdater getBatchUpdater() {
        return updater;
    }

}
