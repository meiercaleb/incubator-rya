package rya.cep.periodic.api;

import org.openrdf.query.BindingSet;

public interface BindingSetExporter {

    public void exportNotification(BindingSet bindingSet);

}
