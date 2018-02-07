package org.opencb.bionetdb.core.utils;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Pair;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.network.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

public class RecordConverter {

    public static List<Node> toNodes(Record record) {
        List<Node> nodes = new ArrayList<>();

        for (Pair<String, Value> pair : record.fields()) {
            if (pair.value().hasType(TYPE_SYSTEM.NODE())) {
                nodes.add(toNode(pair.value().asNode()));
            }
        }

        return nodes;
    }

    public static List<Object> toRow(Record record) {
        List<Object> row = new ArrayList<>();

        for (Pair<String, Value> pair : record.fields()) {
            if (pair.value().hasType(TYPE_SYSTEM.NODE())) {
                row.add(toNode(pair.value().asNode()));
            } else if (pair.value().hasType(TYPE_SYSTEM.RELATIONSHIP())) {
                //row.add(toNode(pair.value().asNode()));
                System.out.println("RELATIONSHIP not yet supported !");
            } else if (pair.value().hasType(TYPE_SYSTEM.PATH())) {
                //row.add(toNode(pair.value().asNode()));
                System.out.println("PATH not yet supported !");
            } else {
                row.add(pair.value());
            }
        }

        return row;
    }

    private static Node toNode(org.neo4j.driver.v1.types.Node neoNode) {
        // Set uid, id and name
        Node node = new Node(neoNode.get("uid").asLong());
        node.setId(neoNode.get("id").asString());
        node.setName(neoNode.get("name").asString());

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
                node.addAttribute(k.substring(pos), neoNode.get(k));
            }
        }
        return node;
    }
}
