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
    public void parseVariantQueryPanelBiotypeConsequenceType() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.PANEL.key(), "Familial or syndromic hypoparathyroidism,Hydrocephalus,Cerebellar hypoplasia");
        query.put(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,polymorphic_pseudogene");
        query.put(Neo4JVariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,intron_variant,");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryBiotypeConsequenceType() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,polymorphic_pseudogene");
        query.put(Neo4JVariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,intron_variant,");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryPopFreq() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "JPN<0.001");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryPopFreqsOR() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "JPN<0.001,AFR>0.3");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryPopFreqsAND() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "JPN<0.001;AFR>0.3");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryPopFreqsORChromosome() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "X");
        query.put(Neo4JVariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "JPN<0.001,AFR>0.3");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryPopFreqsANDChromosome() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "X");
        query.put(Neo4JVariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "JPN<0.001;AFR>0.3");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryGenotypes() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.GENOTYPE.key(), "sample1:1/0,1/1,sample2:1/0");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }

    @Test
    public void parseVariantQueryGenotypesChrom() throws Exception {
        Neo4JQueryParser parser = new Neo4JQueryParser();

        Query query = new Query();
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "M,MT,Mt,mt");
        query.put(Neo4JVariantQueryParam.GENOTYPE.key(), "sample1:1/0,1/1,sample2:1/0");

        String cypher = parser.parseVariantQuery(query, QueryOptions.empty());
        System.out.println(cypher);
    }
}