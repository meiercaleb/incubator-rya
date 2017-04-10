package api;

public class NodeBin {

    private long bin;
    private String nodeId;
    
    public NodeBin(String nodeId, long bin) {
        this.bin = bin;
        this.nodeId = nodeId;
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public long getBin() {
        return bin;
    }
    
}
