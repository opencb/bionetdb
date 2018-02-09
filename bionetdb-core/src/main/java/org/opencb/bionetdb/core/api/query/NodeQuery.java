package org.opencb.bionetdb.core.api.query;

import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.datastore.core.Query;

public class NodeQuery extends Query {

    private Node.Type type;

    public NodeQuery(Node.Type type) {
        this.type = type;
    }

    public Node.Type getType() {
        return type;
    }

    public NodeQuery setType(Node.Type type) {
        this.type = type;
        return this;
    }
}
