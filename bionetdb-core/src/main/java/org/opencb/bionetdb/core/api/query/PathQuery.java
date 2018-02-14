package org.opencb.bionetdb.core.api.query;

import org.opencb.commons.datastore.core.Query;

public class PathQuery extends Query {

    private NodeQuery srcNodeQuery;
    private NodeQuery destNodeQuery;
    private NodeQuery intermediateNodeQuery;

    public PathQuery(NodeQuery srcNodeQuery, NodeQuery destNodeQuery) {
        this(srcNodeQuery, destNodeQuery, null);
    }

    public PathQuery(NodeQuery srcNodeQuery, NodeQuery destNodeQuery, NodeQuery intermediateNodeQuery) {
        this.srcNodeQuery = srcNodeQuery;
        this.destNodeQuery = destNodeQuery;
        this.intermediateNodeQuery = intermediateNodeQuery;
    }

    public NodeQuery getSrcNodeQuery() {
        return srcNodeQuery;
    }

    public PathQuery setSrcNodeQuery(NodeQuery srcNodeQuery) {
        this.srcNodeQuery = srcNodeQuery;
        return this;
    }

    public NodeQuery getDestNodeQuery() {
        return destNodeQuery;
    }

    public PathQuery setDestNodeQuery(NodeQuery destNodeQuery) {
        this.destNodeQuery = destNodeQuery;
        return this;
    }

    public NodeQuery getIntermediateNodeQuery() {
        return intermediateNodeQuery;
    }

    public PathQuery setIntermediateNodeQuery(NodeQuery intermediateNodeQuery) {
        this.intermediateNodeQuery = intermediateNodeQuery;
        return this;
    }
}
