package org.opencb.bionetdb.lib.api.query;

import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.commons.datastore.core.Query;

public class NodeQuery extends Query {

    private Node.Label label;

    public NodeQuery() {
        this.label = null;
    }

    public NodeQuery(Node.Label label) {
        this.label = label;
    }

    public Node.Label getLabel() {
        return label;
    }

    public NodeQuery setLabel(Node.Label label) {
        this.label = label;
        return this;
    }
}
