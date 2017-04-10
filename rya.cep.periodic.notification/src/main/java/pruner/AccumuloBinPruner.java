package pruner;

import java.util.Collections;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;

import api.BinPruner;
import api.NodeBin;
import jline.internal.Preconditions;

/**
 * Deletes BindingSets from time bins in the indicated PCJ table
 */
public class AccumuloBinPruner implements BinPruner {

    private static final Logger log = Logger.getLogger(AccumuloBinPruner.class);
    private static final PcjTableNameFactory pcjTableNameFactory = new PcjTableNameFactory();
    private Connector connector;
    private Authorizations auths;
    private String ryaInstanceName;

    public AccumuloBinPruner(Connector connector, Authorizations auths, String ryaInstanceName) {
        Preconditions.checkNotNull(connector);
        Preconditions.checkNotNull(auths);
        Preconditions.checkNotNull(ryaInstanceName);
        this.connector = connector;
        this.auths = auths;
        this.ryaInstanceName = ryaInstanceName;
    }

    /**
     * This method deletes all BindingSets in the indicated bin from the PCJ
     * table indicated by the id. It is assumed that all BindingSet entries for
     * the corresponding bin are written to the PCJ table so that the bin Id
     * occurs first.
     * 
     * @param id
     *            - pcj table id
     * @param bin
     *            - temporal bin the BindingSets are contained in
     */
    @Override
    public void pruneBindingSetBin(NodeBin nodeBin) {
        Preconditions.checkNotNull(nodeBin);
        String id = nodeBin.getNodeId();
        long bin = nodeBin.getBin();
        try {
            String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, id);
            BatchDeleter deleter = connector.createBatchDeleter(pcjTableName, auths, 2, new BatchWriterConfig());
            Range range = Range.prefix(new Text(Long.toString(bin)));
            deleter.setRanges(Collections.singleton(range));
            deleter.delete();
            deleter.close();
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.trace("Unable to locate PCJ Table: " + id);
            throw new RuntimeException(e);
        }
    }

}
