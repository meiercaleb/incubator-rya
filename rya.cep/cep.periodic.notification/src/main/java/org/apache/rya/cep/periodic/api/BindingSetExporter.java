package org.apache.rya.cep.periodic.api;

import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter.ResultExportException;
import org.apache.rya.periodic.notification.exporter.BindingSetRecord;

public interface BindingSetExporter {

    public void exportNotification(BindingSetRecord bindingSet) throws ResultExportException;

}
