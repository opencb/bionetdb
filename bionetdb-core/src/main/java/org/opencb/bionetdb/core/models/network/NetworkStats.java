package org.opencb.bionetdb.core.models.network;

import java.util.Map;

public class NetworkStats {


    private long nodeCount;
    private long relationCount;

    private long nodeTypeCount;
    private long relationTypeCount;
    private long attributeTypeCount;
    private Map<String, Long> aggNodeTypes;
    private Map<String, Long> aggRelationTypes;

    public NetworkStats() {
    }

    public NetworkStats(long nodeCount, long relationCount, long nodeTypeCount, long relationTypeCount, long attributeTypeCount,
                        Map<String, Long> aggNodeTypes, Map<String, Long> aggRelationTypes) {
        this.nodeCount = nodeCount;
        this.relationCount = relationCount;
        this.nodeTypeCount = nodeTypeCount;
        this.relationTypeCount = relationTypeCount;
        this.attributeTypeCount = attributeTypeCount;
        this.aggNodeTypes = aggNodeTypes;
        this.aggRelationTypes = aggRelationTypes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NetworkStats{");
        sb.append("nodeCount=").append(nodeCount);
        sb.append(", relationCount=").append(relationCount);
        sb.append(", nodeTypeCount=").append(nodeTypeCount);
        sb.append(", relationTypeCount=").append(relationTypeCount);
        sb.append(", attributeTypeCount=").append(attributeTypeCount);
        sb.append(", aggNodeTypes=").append(aggNodeTypes);
        sb.append(", aggRelationTypes=").append(aggRelationTypes);
        sb.append('}');
        return sb.toString();
    }

    public long getNodeCount() {
        return nodeCount;
    }

    public NetworkStats setNodeCount(long nodeCount) {
        this.nodeCount = nodeCount;
        return this;
    }

    public long getRelationCount() {
        return relationCount;
    }

    public NetworkStats setRelationCount(long relationCount) {
        this.relationCount = relationCount;
        return this;
    }

    public long getNodeTypeCount() {
        return nodeTypeCount;
    }

    public NetworkStats setNodeTypeCount(long nodeTypeCount) {
        this.nodeTypeCount = nodeTypeCount;
        return this;
    }

    public long getRelationTypeCount() {
        return relationTypeCount;
    }

    public NetworkStats setRelationTypeCount(long relationTypeCount) {
        this.relationTypeCount = relationTypeCount;
        return this;
    }

    public long getAttributeTypeCount() {
        return attributeTypeCount;
    }

    public NetworkStats setAttributeTypeCount(long attributeTypeCount) {
        this.attributeTypeCount = attributeTypeCount;
        return this;
    }

    public Map<String, Long> getAggNodeTypes() {
        return aggNodeTypes;
    }

    public NetworkStats setAggNodeTypes(Map<String, Long> aggNodeTypes) {
        this.aggNodeTypes = aggNodeTypes;
        return this;
    }

    public Map<String, Long> getAggRelationTypes() {
        return aggRelationTypes;
    }

    public NetworkStats setAggRelationTypes(Map<String, Long> aggRelationTypes) {
        this.aggRelationTypes = aggRelationTypes;
        return this;
    }
}
