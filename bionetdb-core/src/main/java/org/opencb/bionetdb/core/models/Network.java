package org.opencb.bionetdb.core.models;

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
//    private Map<String, Integer> nodesIndex;
    private List<Relationship> relationships;
//    private Map<String, Integer> relationshipsIndex;

    protected Map<String, Object> attributes;

//    protected Type type;
//
//    public enum Type {
//        NODE ("node"),
//        RELATIONSHIP ("relationship");
//
//        private final String type;
//
//        Type(String type) {
//            this.type = type;
//        }
//    }

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
        relationships = new ArrayList<>();

//        nodesIndex = new HashMap<>();
//        relationshipsIndex =new HashMap<>();

        attributes = new HashMap<>();
    }

//    public Node getNode(String id) {
//        return nodes.get(nodesIndex.get(id));
//    }
//
//    public void setNode(Node node) {
//        if (node != null) {
//            if (!nodesIndex.containsKey(node.getId())) {
//                nodes.add(node);
//                nodesIndex.put(node.getId(), nodes.indexOf(node));
//            }
//        }
//    }
//
//    public Relationship getRelationship(String id) {
//        return relationships.get(relationshipsIndex.get(id));
//    }
//
//    public void setRelationship(Relationship relationship) {
//        if (relationship != null) {
//            if (!relationshipsIndex.containsKey(relationship.getId())) {
//                relationships.add(relationship);
//                relationshipsIndex.put(relationship.getId(), relationships.indexOf(relationship));
//            }
//        }
//    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
//        for (Node node: nodes) {
//            nodesIndex.put(node.getId(), this.nodes.indexOf(node));
//        }
    }

    public List<Relationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<Relationship> relationships) {
        this.relationships = relationships;
//        for (Relationship relationship: relationships) {
//            relationshipsIndex.put(relationship.getId(), this.relationships.indexOf(relationship));
//        }
    }

//    public Type getNetworkElementType(String id) {
//        Type elementType = null;
//        if (nodesIndex.containsKey(id)) {
//            elementType = Type.NODE;
//        } else if (relationshipsIndex.containsKey(id)) {
//            elementType = Type.RELATIONSHIP;
//        }
//        return elementType;
//    }

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
