package org.opencb.bionetdb.core.neo4j.query;

import org.junit.Test;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.api.query.PathQuery;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by joaquin on 2/19/18.
 */
public class Neo4JQueryParserTest {
    @Test
    public void parseNode() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();
        NodeQuery query = new NodeQuery(Node.Type.PROTEIN);
        query.put("name", "RDH11");
        String cypher = parser.parseNode(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parsePath() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();
        NodeQuery srcQuery = new NodeQuery(Node.Type.CATALYSIS);
        NodeQuery destQuery = new NodeQuery(Node.Type.SMALL_MOLECULE);
        //query.put("name", "RDH11");
        PathQuery pathQuery = new PathQuery(srcQuery, destQuery, null);
        String cypher = parser.parsePath(pathQuery, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseNodesForNetwork() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();
        List<NodeQuery> nodeQueries = new ArrayList<>();
        nodeQueries.add(new NodeQuery(Node.Type.PROTEIN));
        nodeQueries.add(new NodeQuery(Node.Type.SMALL_MOLECULE));
        nodeQueries.add(new NodeQuery(Node.Type.COMPLEX));
        String cypher = parser.parseNodesForNetwork(nodeQueries, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parsePathsForNetwork() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();
        List<PathQuery> pathQueries = new ArrayList<>();

        pathQueries.add(new PathQuery(new NodeQuery(Node.Type.PROTEIN), new NodeQuery(Node.Type.CATALYSIS), null));
        pathQueries.add(new PathQuery(new NodeQuery(Node.Type.COMPLEX), new NodeQuery(Node.Type.SMALL_MOLECULE), null));
        String cypher = parser.parsePathsForNetwork(pathQueries, QueryOptions.empty());
        System.out.println(cypher);
    }
}