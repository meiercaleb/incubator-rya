package org.apache.rya.periodic.notification.pruner;

import org.apache.log4j.Logger;
import org.apache.rya.cep.periodic.api.BinPruner;
import org.apache.rya.cep.periodic.api.NodeBin;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;

import jline.internal.Preconditions;

/**
 * Deletes BindingSets from time bins in the indicated PCJ table
 */
public class AccumuloBinPruner implements BinPruner {

    private static final Logger log = Logger.getLogger(AccumuloBinPruner.class);
    private PeriodicQueryResultStorage periodicStorage;

    public AccumuloBinPruner(PeriodicQueryResultStorage periodicStorage) {
        Preconditions.checkNotNull(periodicStorage);
        this.periodicStorage = periodicStorage;
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
            periodicStorage.deletePeriodicQueryResults(id, bin);
        } catch (PeriodicQueryStorageException e) {
            log.trace("Unable to delete results from Peroidic Table: " + id + " for bin: " + bin);
            throw new RuntimeException(e);
        }
    }

}
