package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;

public class SpanBatchInformation extends AbstractSpanBatchInformation {

    private static final BatchBindingSetUpdater updater = new SpanBatchBindingSetUpdater();
    
    public SpanBatchInformation(int batchSize, Column column, Span span) {
        super(batchSize, Task.Delete, column, span);
    }
    
    @Override
    public BatchBindingSetUpdater getBatchUpdater() {
        return updater;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {

        private int batchSize = DEFAULT_BATCH_SIZE;
        private Column column;
        private Span span;

        /**
         * @param batchSize
         *            the batchSize to set
         */
        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setColumn(Column column) {
            this.column = column;
            return this;
        }

        /**
         * @param span
         *            the span to set
         */
        public Builder setSpan(Span span) {
            this.span = span;
            return this;
        }


        public SpanBatchInformation build() {
            return new SpanBatchInformation(batchSize, column, span);
        }

    }


}
