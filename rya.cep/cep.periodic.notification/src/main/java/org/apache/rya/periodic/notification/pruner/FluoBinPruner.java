package org.apache.rya.periodic.notification.pruner;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.BinPruner;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Optional;

public class FluoBinPruner implements BinPruner {

    private static final Logger log = Logger.getLogger(FluoBinPruner.class);
    private FluoClient client;

    public FluoBinPruner(FluoClient client) {
        this.client = client;
    }

    /**
     * This method deletes BindingSets in the specified bin from the BindingSet
     * Column of the indicated Fluo nodeId
     * 
     * @param id
     *            - Fluo nodeId
     * @param bin
     *            - bin id
     */
    @Override
    public void pruneBindingSetBin(NodeBin nodeBin) {
        String id = nodeBin.getNodeId();
        long bin = nodeBin.getBin();
        try (Transaction tx = client.newTransaction()) {
            Optional<NodeType> type = NodeType.fromNodeId(id);
            if (!type.isPresent()) {
                log.trace("Unable to determine NodeType from id: " + id);
                throw new RuntimeException();
            }
            Column batchInfoColumn = type.get().getBsColumn();
            String batchInfoSpanPrefix = id + IncrementalUpdateConstants.NODEID_BS_DELIM + bin;
            SpanBatchDeleteInformation batchInfo = SpanBatchDeleteInformation.builder().setColumn(batchInfoColumn)
                    .setSpan(Span.prefix(Bytes.of(batchInfoSpanPrefix))).build();
            byte[] batchBytes = BatchInformationSerializer.toBytes(batchInfo);
            tx.set(Bytes.of(id), FluoQueryColumns.BATCH_COLUMN, Bytes.of(batchBytes));
            tx.commit();
        }
    }

}
