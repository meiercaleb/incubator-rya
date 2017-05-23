package org.apache.rya.indexing.pcj.fluo.app.observers;

import static java.util.Objects.requireNonNull;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;

public class PeriodicQueryObserver extends BindingSetUpdater {

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.PERIODIC_QUERY_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final Bytes row) throws Exception {
        requireNonNull(tx);
        requireNonNull(row);

        // Read the Join metadata.
        final String periodicBinNodeId = BindingSetRow.make(row).getNodeId();
        final PeriodicQueryMetadata periodicBinMetadata = queryDao.readPeriodicQueryMetadata(tx, periodicBinNodeId);

        // Read the Visibility Binding Set from the Value.
        final Bytes valueBytes = tx.get(row, FluoQueryColumns.PERIODIC_QUERY_BINDING_SET);
        final VisibilityBindingSet periodicBinBindingSet = BS_SERDE.deserialize(valueBytes);

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = periodicBinMetadata.getParentNodeId();

        return new Observation(periodicBinNodeId, periodicBinBindingSet, parentNodeId);
    }
    

}
