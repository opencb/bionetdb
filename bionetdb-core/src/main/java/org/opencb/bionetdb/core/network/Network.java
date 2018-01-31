package org.opencb.bionetdb.core.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 10/08/15.
 */
public class Network {

    private String id;
    private String name;
    private String description;

    private List<Node> nodes;
    private List<Relation> relations;

    private Map<String, Object> attributes;

    public Network() {
        this.id = "";
        this.name = "";
        this.description = "";

        // init rest of attributes
        init();
    }

    public Network(long uid, String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;

        // init rest of attributes
        init();
    }

    private void init() {
        nodes = new ArrayList<>();
        relations = new ArrayList<>();

        attributes = new HashMap<>();
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void addRelationship(Relation relation) {
        relations.add(relation);
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Relation> getRelations() {
        return relations;
    }

    public void setRelations(List<Relation> relations) {
        this.relations = relations;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
