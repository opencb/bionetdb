package org.opencb.bionetdb.core.models.network;

import java.util.Map;

public class NodeStats {

    private long numNodes;
    private Map<String, Long> labelCount;

    public NodeStats() {
    }

    public NodeStats(long numNodes, Map<String, Long> labelCount) {
        this.numNodes = numNodes;
        this.labelCount = labelCount;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NodeStats{");
        sb.append("numNodes=").append(numNodes);
        sb.append(", labelCount=").append(labelCount);
        sb.append('}');
        return sb.toString();
    }

    public long getNumNodes() {
        return numNodes;
    }

    public NodeStats setNumNodes(long numNodes) {
        this.numNodes = numNodes;
        return this;
    }

    public Map<String, Long> getLabelCount() {
        return labelCount;
    }

    public NodeStats setLabelCount(Map<String, Long> labelCount) {
        this.labelCount = labelCount;
        return this;
    }
}
