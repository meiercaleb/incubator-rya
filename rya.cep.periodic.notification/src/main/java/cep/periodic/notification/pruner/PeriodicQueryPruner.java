package cep.periodic.notification.pruner;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Optional;

import jline.internal.Preconditions;
import rya.cep.periodic.api.BinPruner;
import rya.cep.periodic.api.NodeBin;

public class PeriodicQueryPruner implements BinPruner, Runnable {

    private static final Logger log = Logger.getLogger(PeriodicQueryPruner.class);
    private FluoClient client;
    private AccumuloBinPruner accPruner;
    private FluoBinPruner fluoPruner;
    private BlockingQueue<NodeBin> bins;
    private int threadNumber;

    public PeriodicQueryPruner(FluoBinPruner fluoPruner, AccumuloBinPruner accPruner, FluoClient client, BlockingQueue<NodeBin> bins, int threadNumber) {
        Preconditions.checkNotNull(fluoPruner);
        Preconditions.checkNotNull(accPruner);
        Preconditions.checkNotNull(client);
        this.client = client;
        this.accPruner = accPruner;
        this.fluoPruner = fluoPruner;
        this.bins = bins;
        this.threadNumber = threadNumber;
    }
    
    @Override
    public void run() {
        try {
            pruneBindingSetBin(bins.take());
        } catch (InterruptedException e) {
            log.trace("Thread " + threadNumber + " is unable to prune the next message.");
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Prunes BindingSet bins from the Rya Fluo Application in addition to the BindingSet
     * bins created in the PCJ tables associated with the give query id.
     * @param id - QueryResult Id for the Rya Fluo application 
     * @param bin - bin id for bins to be deleted
     */
    @Override
    public void pruneBindingSetBin(NodeBin nodeBin) {
        String id = nodeBin.getNodeId();
        long bin = nodeBin.getBin();
        try(Snapshot sx = client.newSnapshot()) {
            String pcjId = sx.get(Bytes.of(id), FluoQueryColumns.RYA_PCJ_ID).toString();
            Set<String> fluoIds = getNodeIdsFromResultId(sx, id);
            accPruner.pruneBindingSetBin(new NodeBin(pcjId, bin));
            for(String fluoId: fluoIds) {
                fluoPruner.pruneBindingSetBin(new NodeBin(fluoId, bin));
            }
        } catch (Exception e) {
            log.trace("Could not successfully initialize PeriodicQueryBinPruner.");
        }
    }

    private Set<String> getNodeIdsFromResultId(SnapshotBase sx, String id) {
        Set<String> ids = new HashSet<>();
        getIds(sx, id, ids);
        return ids;
    }

    private void getIds(SnapshotBase sx, String nodeId, Set<String> ids) {
        Optional<NodeType> nodeType = NodeType.fromNodeId(nodeId);
        checkArgument(nodeType.isPresent(), "Invalid nodeId: " + nodeId + ". NodeId does not correspond to a valid NodeType.");
        NodeType type = nodeType.get();
        switch (type) {
        case FILTER:
            ids.add(nodeId);
            getIds(sx, sx.get(Bytes.of(nodeId), FluoQueryColumns.FILTER_CHILD_NODE_ID).toString(), ids);
            break;
        case PERIODIC_BIN:
            ids.add(nodeId);
            break;
        case QUERY:
            ids.add(nodeId);
            getIds(sx, sx.get(Bytes.of(nodeId), FluoQueryColumns.QUERY_CHILD_NODE_ID).toString(), ids);
            break;
        default:
            log.trace("Invalid query structure.  PerioidicBinNode is only allowed to have Filters and QueryNodes as ancestors.");
            throw new RuntimeException();
        }

    }

}
