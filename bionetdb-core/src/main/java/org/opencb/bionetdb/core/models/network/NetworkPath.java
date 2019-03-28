package org.opencb.bionetdb.core.models.network;

/**
 * Created by joaquin on 2/12/18.
 */
public class NetworkPath extends Graph {
    private int startIndex;
    private int endIndex;

    public NetworkPath() {
        super();
        startIndex = 0;
        endIndex = 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NetworkPath{");
        sb.append("startIndex=").append(startIndex);
        sb.append(", endIndex=").append(endIndex);
        sb.append(", nodes=").append(nodes);
        sb.append(", relations=").append(relations);
        sb.append('}');
        return sb.toString();
    }

    public int getStartIndex() {
        return startIndex;
    }

    public NetworkPath setStartIndex(int startIndex) {
        this.startIndex = startIndex;
        return this;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public NetworkPath setEndIndex(int endIndex) {
        this.endIndex = endIndex;
        return this;
    }
}
