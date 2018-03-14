package org.opencb.bionetdb.core;

import org.junit.Before;
import org.junit.Test;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class BioNetDBManagerTest {

    private String database = "scerevisiae";
    private BioNetDBManager bioNetDBManager;


    @Before
    public void initialize () {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig: bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }
            bioNetDBManager = new BioNetDBManager(database, bioNetDBConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (BioNetDBException e) {
            e.printStackTrace();
        }

//        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        clientConfiguration.setVersion("v4");
//        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
//        cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    @Test
    public void loadBioPax() throws BioNetDBException, IOException {
        String root = "/home/jtarraga/data150/neo4j"; // "~"; // ~/data150/neo4j
        String filename = getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").getPath();
        //String filename = root + "/data150/neo4j/hsapiens.meiosis.biopax3";
        //String filename = root + "/pathway1.biopax3";
        //String filename = root + "/vesicle.mediated.transport.biopax3";
        //String filename = root + "/Homo_sapiens.owl";
        bioNetDBManager.loadBioPax(Paths.get(filename));
    }

    @Test
    public void loadVCF() throws BioNetDBException {
        try {
            bioNetDBManager.loadVcf(Paths.get(getClass().getResource("/3.vcf").toURI()));
        } catch (URISyntaxException e) {
            e.printStackTrace();
            fail();
        }
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    @Test
    public void annotateVariants() throws BioNetDBException, IOException {
        loadVCF();

        List<String> ids = new ArrayList<>();

//        variantIds.add("rs540431307");
//        variantIds.add("rs367896724");
//        variantIds.add("rs429358");

        QueryResult<Node> queryResult = bioNetDBManager.nodeQuery("MATCH (n:VARIANT) return n");
        if (queryResult != null && ListUtils.isNotEmpty(queryResult.getResult())) {
            for (Node node: queryResult.getResult()) {
                ids.add(node.getId());
            }
        }

        bioNetDBManager.annotateVariants(ids);
    }

    @Test
    public void annotateGenes() throws BioNetDBException, IOException {
        annotateVariants();
        List<String> ids = new ArrayList<>();

//        geneIds.add("ENSG00000227232");
//        geneIds.add("ENSG00000223972");

        QueryResult<Node> queryResult = bioNetDBManager.nodeQuery("MATCH (n:GENE) return n");
        if (queryResult != null && ListUtils.isNotEmpty(queryResult.getResult())) {
            for (Node node: queryResult.getResult()) {
                ids.add(node.getId());
            }
        }

        bioNetDBManager.annotateGenes(ids);
    }

    @Test
    public void annotateProtein() throws BioNetDBException, IOException {
        //annotateGenes();

        List<String> ids = new ArrayList<>();

        QueryResult<Node> queryResult = bioNetDBManager.nodeQuery("MATCH (n:PROTEIN) return n");
        if (queryResult != null && ListUtils.isNotEmpty(queryResult.getResult())) {
            for (Node node: queryResult.getResult()) {
                ids.add(node.getId());
            }
        }

//        ids.add("P02649");
//        ids.add("HIST2H3A");

        bioNetDBManager.annotateProteins(ids);
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    @Test
    public void getNodeByUid() throws Exception {
        QueryResult<Node> queryResult = bioNetDBManager.getNode(1);
        printNodes(queryResult.getResult());
    }

    @Test
    public void nodeQuery() throws Exception {
        // match path=(g:GENE)-[r]-(t:TRANSCRIPT) where g.name="APOE" return path
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
//        query.put(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key(), "TRANSCRIPT");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "GENE,TRANSCRIPT");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        QueryResult<Node> queryResult = bioNetDBManager.nodeQuery(cypher.toString());
        printNodes(queryResult.getResult());
    }

    @Test
    public void nodeQueryByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match (g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return g,t");
        QueryResult<Node> queryResult = bioNetDBManager.nodeQuery(cypher.toString());
        printNodes(queryResult.getResult());
    }

    @Test
    public void tableQuery() throws Exception {
        // match path=(g:GENE)-[r]-(t:TRANSCRIPT) where g.name="APOE" return path
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
//        query.put(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key(), "TRANSCRIPT");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "GENE.name,GENE.id,TRANSCRIPT.id");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        QueryResult<List<Object>> queryResult = bioNetDBManager.table(cypher);
        printLists(queryResult.getResult());
    }

    @Test
    public void tableByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match (g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return g.uid, g.id, t.uid, t.id, g");
        QueryResult<List<Object>> queryResult = bioNetDBManager.table(cypher.toString());
        printLists(queryResult.getResult());
    }

    @Test
    public void networkQuery() throws Exception {
        // match path=(g:GENE)-[r]-(t:TRANSCRIPT) where g.name="APOE" return path
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
        query.put(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key(), "TRANSCRIPT");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "network");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        QueryResult<Network> queryResult = bioNetDBManager.networkQuery(cypher);
        System.out.println(queryResult.getResult().get(0).toString());
    }

    @Test
    public void networkByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match path=(g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return path");
        QueryResult<Network> queryResult = bioNetDBManager.networkQuery(cypher.toString());
        System.out.println(queryResult.getResult().get(0).toString());
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    private void printNodes(List<Node> nodes) {
        for (Node node: nodes) {
            System.out.println(node.toStringEx());
        }
    }

    private void printLists(List<List<Object>> lists) {
        for (List<Object> list: lists) {
            for (Object item: list) {
                System.out.print(item + ", ");
            }
            System.out.println();
        }
    }
}