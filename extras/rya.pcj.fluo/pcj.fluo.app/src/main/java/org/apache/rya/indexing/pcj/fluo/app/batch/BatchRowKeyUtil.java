package org.apache.rya.indexing.pcj.fluo.app.batch;

import java.util.UUID;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;

import com.google.common.base.Preconditions;

public class BatchRowKeyUtil {

    public static Bytes getRow(String nodeId) {
        String row = new StringBuilder().append(nodeId).append(IncrementalUpdateConstants.NODEID_BS_DELIM)
                .append(UUID.randomUUID().toString().replace("-", "")).toString();
        return Bytes.of(row);
    }
    
    public static Bytes getRow(String nodeId, String batchId) {
        String row = new StringBuilder().append(nodeId).append(IncrementalUpdateConstants.NODEID_BS_DELIM)
                .append(batchId).toString();
        return Bytes.of(row);
    }
    
    public static String getNodeId(Bytes row) {
        String[] stringArray = row.toString().split(IncrementalUpdateConstants.NODEID_BS_DELIM);;
        Preconditions.checkArgument(stringArray.length == 2);
        return stringArray[0];
    }
    
}
