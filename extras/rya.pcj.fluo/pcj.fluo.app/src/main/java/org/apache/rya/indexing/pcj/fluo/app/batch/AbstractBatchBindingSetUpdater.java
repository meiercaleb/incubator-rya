package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;

public abstract class AbstractBatchBindingSetUpdater implements BatchBindingSetUpdater {

    public Span getNewSpan(RowColumn newStart, Span oldSpan) {
        return new Span(newStart, oldSpan.isStartInclusive(), oldSpan.getEnd(), oldSpan.isEndInclusive());
    }


}
