package org.opencb.bionetdb.core.network;

import java.util.List;

/**
 * Created by joaquin on 2/12/18.
 */
public class Graph {
    protected List<Node> nodes;
    protected List<Relation> relations;

    public List<Node> getNodes() {
        return nodes;
    }

    public Graph setNodes(List<Node> nodes) {
        this.nodes = nodes;
        return this;
    }

    public List<Relation> getRelations() {
        return relations;
    }

    public Graph setRelations(List<Relation> relations) {
        this.relations = relations;
        return this;
    }
}
