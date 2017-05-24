package org.apache.rya.cep.periodic.api;

import java.util.Objects;

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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof NodeBin) {
            NodeBin bin = (NodeBin) other;
            return this.bin == bin.bin && this.nodeId.equals(bin.nodeId);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bin, nodeId);
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Node Bin \n").append("   QueryId: " + nodeId + "\n").append("   Bin: " + bin + "\n").toString();
    }

}
