package org.opencb.bionetdb.core.models.network;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by joaquin on 2/12/18.
 */
public class Graph {
    protected List<Node> nodes;
    protected List<Relation> relations;

    public Graph() {
        nodes = new ArrayList<>();
        relations = new ArrayList<>();
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void addRelation(Relation relation) {
        relations.add(relation);
    }

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
