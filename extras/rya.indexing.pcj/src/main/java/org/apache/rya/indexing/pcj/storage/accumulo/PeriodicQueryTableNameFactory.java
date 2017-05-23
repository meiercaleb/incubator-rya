package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

public class PeriodicQueryTableNameFactory {

    public static final String PeriodicTableSuffix = "PERIODIC_QUERY_";
    
    public String makeTableName(final String ryaInstance, final String queryId) {
        requireNonNull(ryaInstance);
        requireNonNull(queryId);
        return ryaInstance + PeriodicTableSuffix + queryId.toString().replaceAll("-", "");
    }

    public String getPeriodicQueryId(final String periodTableName) {
        requireNonNull(periodTableName);
        return periodTableName.split(PeriodicTableSuffix)[1];
    }
    
}
