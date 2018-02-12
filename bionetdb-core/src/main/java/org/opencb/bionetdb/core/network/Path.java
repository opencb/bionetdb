package org.opencb.bionetdb.core.network;

/**
 * Created by joaquin on 2/12/18.
 */
public class Path extends Graph {
    private int startIndex;
    private int endIndex;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Path{");
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

    public Path setStartIndex(int startIndex) {
        this.startIndex = startIndex;
        return this;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public Path setEndIndex(int endIndex) {
        this.endIndex = endIndex;
        return this;
    }
}
