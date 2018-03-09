package org.opencb.bionetdb.core.utils;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Pair;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;

import java.util.*;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

public class Neo4JConverter {

    public static List<Node> toNodeList(Record record) {
        List<Node> nodes = new ArrayList<>();

        for (Pair<String, Value> pair: record.fields()) {
            if (pair.value().hasType(TYPE_SYSTEM.NODE())) {
                nodes.add(toNode(pair.value().asNode()));
            }
        }

        return nodes;
    }

    public static List<Object> toObjectList(Record record) {
        List<Object> row = new ArrayList<>();

        for (Pair<String, Value> pair: record.fields()) {
            if (pair.value().hasType(TYPE_SYSTEM.NODE()) || pair.value().hasType(TYPE_SYSTEM.RELATIONSHIP())
                    || pair.value().hasType(TYPE_SYSTEM.PATH())) {
                // Skip nodes, relationships and paths
                continue;
            } else {
                row.add(pair.value().asObject());
            }
        }

        return row;
    }

    public static List<org.opencb.bionetdb.core.network.Path> toPathList(Record record) {
        List<org.opencb.bionetdb.core.network.Path> paths = new ArrayList<>();

        for (Pair<String, Value> pair: record.fields()) {
            if (pair.value().hasType(TYPE_SYSTEM.PATH())) {
                paths.add(toPath(pair.value().asPath()));
            }
        }

        return paths;
    }

    public static Network toNetwork(StatementResult statementResult) {
        Network network = new Network();

        // First, be sure to process nodes first
        Map<Long, Node> nodeMap = new HashMap<>();
        Map<Long, Relationship> relationshipMap = new HashMap<>();
        while (statementResult.hasNext()) {
            Record record = statementResult.next();
            for (Pair<String, Value> pair: record.fields()) {
                if (pair.value().hasType(TYPE_SYSTEM.NODE())) {
                    org.neo4j.driver.v1.types.Node neoNode = pair.value().asNode();
                    Node node = toNode(neoNode);
                    nodeMap.put(neoNode.id(), node);
                } else if (pair.value().hasType(TYPE_SYSTEM.RELATIONSHIP())) {
                        Relationship neoRelation = pair.value().asRelationship();
                        relationshipMap.put(neoRelation.id(), neoRelation);
                } else if (pair.value().hasType(TYPE_SYSTEM.PATH())) {
                    Path path = pair.value().asPath();
                    getPathContent(path, nodeMap, relationshipMap);
                }
            }
        }

        // Now, we can add these nodes to the network
        for (long key: nodeMap.keySet()) {
            network.addNode(nodeMap.get(key));
        }

        // Then, we can process relationships and insert them into the network
        for (long key: relationshipMap.keySet()) {
            Relationship neoRelation = relationshipMap.get(key);
            Relation relation = new Relation(neoRelation.id(), neoRelation.get("name").asString(),
                    nodeMap.get(neoRelation.startNodeId()).getUid(), nodeMap.get(neoRelation.startNodeId()).getType(),
                    nodeMap.get(neoRelation.endNodeId()).getUid(), nodeMap.get(neoRelation.endNodeId()).getType(),
                    Relation.Type.valueOf(neoRelation.type()));
            network.addRelation(relation);
        }

        // Create network from node and relation maps
        return network;
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private static Node toNode(org.neo4j.driver.v1.types.Node neoNode) {
        // Set uid, id and name
        Node node = new Node(neoNode.id());
        if (neoNode.containsKey("id")) {
            node.setId(neoNode.get("id").asString());
        }
        if (neoNode.containsKey("name")) {
            node.setName(neoNode.get("name").asString());
        }
        if (neoNode.containsKey("source")) {
            node.setSource(neoNode.get("source").asString());
        }

        // Set type and tags
        boolean first = true;
        Iterator<String> iterator = neoNode.labels().iterator();
        while (iterator.hasNext()) {
            if (first) {
                node.setType(Node.Type.valueOf(iterator.next()));
                first = false;
            } else {
                node.addTag(iterator.next());
            }
        }

        // Set attributes
        int pos = Neo4JNetworkDBAdaptor.PREFIX_ATTRIBUTES.length();
        for (String k : neoNode.keys()) {
            if (k.startsWith(Neo4JNetworkDBAdaptor.PREFIX_ATTRIBUTES)) {
                    node.addAttribute(k.substring(pos), neoNode.get(k).asObject());
            }
        }
        return node;
    }

    private static org.opencb.bionetdb.core.network.Path toPath(Path neoPath) {
        org.opencb.bionetdb.core.network.Path path = new org.opencb.bionetdb.core.network.Path();

        // First, be sure to process nodes first
        Map<Long, Node> nodeMap = new HashMap<>();
        Map<Long, Relationship> relationshipMap = new HashMap<>();
        getPathContent(neoPath, nodeMap, relationshipMap);

        // Now, we can add these nodes to the path and set the start and end node indices
        int i = 0;
        int startIndex = 0, endIndex = 0;
        for (long key: nodeMap.keySet()) {
            if (neoPath.start().id() == key) {
                startIndex = i;
            }
            if (neoPath.end().id() == key) {
                endIndex = i;
            }
            path.addNode(nodeMap.get(key));
            i++;
        }
        path.setStartIndex(startIndex);
        path.setEndIndex(endIndex);

        // Then, we can process relationships and insert them into the path
        for (long key: relationshipMap.keySet()) {
            Relationship neoRelation = relationshipMap.get(key);
            Relation relation = new Relation(neoRelation.id(), neoRelation.get("name").asString(),
                    nodeMap.get(neoRelation.startNodeId()).getUid(), nodeMap.get(neoRelation.startNodeId()).getType(),
                    nodeMap.get(neoRelation.endNodeId()).getUid(), nodeMap.get(neoRelation.endNodeId()).getType(),
                    Relation.Type.valueOf(neoRelation.type()));
            path.addRelation(relation);
        }

        // Return path
        return path;
    }

    private static void getPathContent(Path neoPath, Map<Long, Node> nodeMap, Map<Long, Relationship> relationshipMap) {
        if (neoPath.nodes() != null) {
            Iterator<org.neo4j.driver.v1.types.Node> iterator = neoPath.nodes().iterator();
            while (iterator.hasNext()) {
                org.neo4j.driver.v1.types.Node neoNode = iterator.next();
                Node node = toNode(neoNode);
                nodeMap.put(neoNode.id(), node);
            }
        }
        if (neoPath.relationships() != null) {
            Iterator<org.neo4j.driver.v1.types.Relationship> iterator = neoPath.relationships().iterator();
            while (iterator.hasNext()) {
                Relationship neoRelation = iterator.next();
                relationshipMap.put(neoRelation.id(), neoRelation);
            }
        }
    }
}
