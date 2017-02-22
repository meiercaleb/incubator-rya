package org.apache.rya.indexing.pcj.fluo.app.observers;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicBinMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;

public class PeriodicBinObserver extends BindingSetUpdater {

    private final VisibilityBindingSetStringConverter converter = new VisibilityBindingSetStringConverter();

    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.PERIODIC_BIN_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final BindingSetRow parsedRow) {
        checkNotNull(parsedRow);

        // Read the Join metadata.
        final String periodicBinNodeId = parsedRow.getNodeId();
        final PeriodicBinMetadata periodicBinMetadata = queryDao.readPeriodicBinMetadata(tx, periodicBinNodeId);

        // Read the Binding Set that was just emmitted by the Join.
        final VariableOrder periodicBinVarOrder = periodicBinMetadata.getVariableOrder();
        final VisibilityBindingSet periodicBinBindingSet = (VisibilityBindingSet) converter.convert(parsedRow.getBindingSetString(), periodicBinVarOrder);

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = periodicBinMetadata.getParentNodeId();

        return new Observation(periodicBinNodeId, periodicBinBindingSet, parentNodeId);
    }

}
