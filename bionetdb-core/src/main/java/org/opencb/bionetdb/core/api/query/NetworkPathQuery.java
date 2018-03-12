package org.opencb.bionetdb.core.api.query;

import org.opencb.commons.datastore.core.Query;

public class NetworkPathQuery extends Query {

    private NodeQuery srcNodeQuery;
    private NodeQuery destNodeQuery;
    private NodeQuery intermediateNodeQuery;

    public NetworkPathQuery(NodeQuery srcNodeQuery, NodeQuery destNodeQuery) {
        this(srcNodeQuery, destNodeQuery, null);
    }

    public NetworkPathQuery(NodeQuery srcNodeQuery, NodeQuery destNodeQuery, NodeQuery intermediateNodeQuery) {
        this.srcNodeQuery = srcNodeQuery;
        this.destNodeQuery = destNodeQuery;
        this.intermediateNodeQuery = intermediateNodeQuery;
    }

    public NodeQuery getSrcNodeQuery() {
        return srcNodeQuery;
    }

    public NetworkPathQuery setSrcNodeQuery(NodeQuery srcNodeQuery) {
        this.srcNodeQuery = srcNodeQuery;
        return this;
    }

    public NodeQuery getDestNodeQuery() {
        return destNodeQuery;
    }

    public NetworkPathQuery setDestNodeQuery(NodeQuery destNodeQuery) {
        this.destNodeQuery = destNodeQuery;
        return this;
    }

    public NodeQuery getIntermediateNodeQuery() {
        return intermediateNodeQuery;
    }

    public NetworkPathQuery setIntermediateNodeQuery(NodeQuery intermediateNodeQuery) {
        this.intermediateNodeQuery = intermediateNodeQuery;
        return this;
    }
}
