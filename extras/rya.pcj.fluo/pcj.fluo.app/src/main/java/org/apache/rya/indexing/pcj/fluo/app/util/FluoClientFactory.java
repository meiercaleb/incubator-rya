package org.apache.rya.indexing.pcj.fluo.app.util;

import java.util.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;

public class FluoClientFactory {

    public static FluoClient getFluoClient(String appName, Optional<String> tableName, AccumuloRdfConfiguration conf) {
        FluoConfiguration fluoConfig = new FluoConfiguration();
        fluoConfig.setAccumuloInstance(conf.getAccumuloInstance());
        fluoConfig.setAccumuloUser(conf.getAccumuloUser());
        fluoConfig.setAccumuloPassword(conf.getAccumuloPassword());
        fluoConfig.setInstanceZookeepers(conf.getAccumuloZookeepers() + "/fluo");
        fluoConfig.setAccumuloZookeepers(conf.getAccumuloZookeepers());
        fluoConfig.setApplicationName(appName);
        if (tableName.isPresent()) {
            fluoConfig.setAccumuloTable(tableName.get());
        } else {
            fluoConfig.setAccumuloTable(appName);
        }
        return new FluoClientImpl(fluoConfig);
    }
}
