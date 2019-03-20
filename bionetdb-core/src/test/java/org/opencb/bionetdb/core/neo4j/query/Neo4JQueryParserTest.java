package org.opencb.bionetdb.core.neo4j.query;

import org.junit.Test;
import org.opencb.bionetdb.core.api.query.NetworkPathQuery;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by joaquin on 2/19/18.
 */
public class Neo4JQueryParserTest {
    @Test
    public void parseNode() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();
        NodeQuery query = new NodeQuery(Node.Type.PROTEIN);
        query.put("name", "RDH11");
        String cypher = parser.parseNodeQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parsePath() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();
        NodeQuery srcQuery = new NodeQuery(Node.Type.CATALYSIS);
        NodeQuery destQuery = new NodeQuery(Node.Type.SMALL_MOLECULE);
        //query.put("name", "RDH11");
        NetworkPathQuery networkPathQuery = new NetworkPathQuery(srcQuery, destQuery, null);
        String cypher = parser.parsePath(networkPathQuery, QueryOptions.empty());
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
        List<NetworkPathQuery> pathQueries = new ArrayList<>();

        pathQueries.add(new NetworkPathQuery(new NodeQuery(Node.Type.PROTEIN), new NodeQuery(Node.Type.CATALYSIS), null));
        pathQueries.add(new NetworkPathQuery(new NodeQuery(Node.Type.COMPLEX), new NodeQuery(Node.Type.SMALL_MOLECULE), null));
        String cypher = parser.parsePathsForNetwork(pathQueries, QueryOptions.empty());
        System.out.println(cypher);
    }


    @Test
    public void parseVariantQuery1() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put("panel", "Familial or syndromic hypoparathyroidism,Hydrocephalus,Cerebellar hypoplasia");
        query.put("biotype", "protein_coding,polymorphic_pseudogene");
        query.put("consequenceType", "missense_variant,intron_variant,");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQuery2() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put("biotype", "protein_coding,polymorphic_pseudogene");
        query.put("consequenceType", "missense_variant,intron_variant,");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }
}