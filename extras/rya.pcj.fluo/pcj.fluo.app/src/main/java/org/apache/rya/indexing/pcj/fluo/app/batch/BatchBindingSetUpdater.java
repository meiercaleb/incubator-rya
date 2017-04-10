package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.client.TransactionBase;

public interface BatchBindingSetUpdater {

    public void processBatch(TransactionBase tx, String nodeId, BatchInformation batch);
    
}
