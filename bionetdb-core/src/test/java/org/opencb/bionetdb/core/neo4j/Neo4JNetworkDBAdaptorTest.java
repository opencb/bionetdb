package org.opencb.bionetdb.core.neo4j;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.network.Network;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 21/08/15.
 */
public class Neo4JNetworkDBAdaptorTest {

    String database = "scerevisiae";
    NetworkDBAdaptor networkDBAdaptor = null;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void initialize () {
        // Remove existing database
//        try {
//            deleteRecursively(new File(database));
//        } catch ( IOException e ) {
//            throw new RuntimeException( e );
//        }
        // Create again the path to the database
        new File(database).mkdirs();
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig: bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration, true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (BioNetDBException e) {
            e.printStackTrace();
        }
    }

    @After
    public void close() throws Exception {
        networkDBAdaptor.close();
    }

    @Test
    public void testInsert() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        String reactomeBiopaxFilename = "/home/jtarraga/data150/neo4j/pathway1.biopax3";
        //Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Path inputPath = Paths.get(reactomeBiopaxFilename);
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        System.out.println("Inserting data...");
        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        long stopTime = System.currentTimeMillis();
        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
//        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes introduced in the database is not correct", 28963, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships introduced in the database is not correct", 84278, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));

//        System.out.println("Inserting the same data...");
//        startTime = System.currentTimeMillis();
//        networkDBAdaptor.insert(network, null);
//        stopTime = System.currentTimeMillis();
//        System.out.println("Trying to insert the same data took " + (stopTime - startTime)/1000 + " seconds.");
//        myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes introduced in the database is not correct", 28963, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships introduced in the database is not correct", 84278, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
    }

//    @Test
//    public void testAddExpressionData() throws Exception {
//        BioPaxParser bioPaxParser = new BioPaxParser("L3");
//        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
//        Network network = bioPaxParser.parse(inputPath);
//        System.out.println("The file has been parsed.");
//
//        QueryOptions options = new QueryOptions("addNodes", false);
//
//        networkDBAdaptor.insert(network, null);
//        System.out.println("Data has been inserted in the database.");
//
//        System.out.println("Starting test to add expression data...");
//        Path metadata = Paths.get(getClass().getResource("/expression/myfactors.txt").toURI());
//        ExpressionParser expressionParser = new ExpressionParser(metadata);
//        Map<String, Map<String, String>> allExpressionFiles = expressionParser.getMyFiles();
//        for (String tissue : allExpressionFiles.keySet()) {
//            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
//                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
//                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
//            }
//        }
//        System.out.println("Expression data has been inserted in the database.");
//        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes after inserting the expression data is not correct", 27933, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships after inserting the expression data is not correct", 40424, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
//
//        for (String tissue : allExpressionFiles.keySet()) {
//            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
//                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
//                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
//            }
//        }
//        System.out.println("The same expression data has been tried to be inserted in the database.");
//        myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes after inserting the expression data is not correct", 27933, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships after inserting the expression data is not correct", 40424, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
//
//        options.put("addNodes", true);
//        for (String tissue : allExpressionFiles.keySet()) {
//            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
//                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
//                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
//            }
//        }
//        System.out.println("The same expression data allowing the annotation of nodes not in the database has been tried to be inserted in the database.");
//        myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes after inserting the expression data is not correct", 27940, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships after inserting the expression data is not correct", 40430, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
//
//    }
//
//    @Test
//    public void testAddXrefs() throws Exception {
//        BioPaxParser bioPaxParser = new BioPaxParser("L3");
//        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
//        Network network = bioPaxParser.parse(inputPath);
//        System.out.println("The file has been parsed.");
//
//        networkDBAdaptor.insert(network, null);
//        System.out.println("Data has been inserted in the database.");
//
//        System.out.println("Starting test of annotation of xref elements...");
//        List<Xref> myList = new ArrayList<>();
//        for (int i = 0; i < 4; i++) {
//            myList.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
//        }
//        networkDBAdaptor.addXrefs("CMK1", myList);
//        System.out.println("New xrefs added to the database.");
//        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes after inserting the expression data is not correct", 27897, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships after inserting the expression data is not correct", 40416, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
//
//        System.out.println("Creating new xrefs containing one that was already inserted...");
//        myList.clear();
//        for (int i = 3; i < 8; i++) {
//            myList.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
//        }
//        networkDBAdaptor.addXrefs("id2", myList);
//        System.out.println("New xrefs added to the database.");
//        myResult = networkDBAdaptor.getSummaryStats(null, null);
//        assertEquals("The number of nodes after inserting the expression data is not correct", 27901, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
//        assertEquals("The number of relationships after inserting the expression data is not correct", 40448, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
//    }
//
//    @Test
//    public void testGetSummaryStats() throws Exception {
////        BioPaxParser bioPaxParser = new BioPaxParser("L3");
////        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
////        Network network = bioPaxParser.parse(inputPath);
////        System.out.println("The file has been parsed.");
////
////        networkDBAdaptor.insert(network, null);
////        System.out.println("Data has been inserted in the database.");
//
//        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
//        System.out.println("Querying the database to retrieve the stats took " + (myResult.getDbTime() / 1000.0) + " seconds");
//        ObjectMap myStats = (ObjectMap) myResult.getResult().get(0);
//        for (String key : myStats.keySet()) {
//            System.out.println(key + ": " + myStats.get(key));
//        }
//
//        networkDBAdaptor.close();
////        exception.expect(TransactionFailureException.class);
////        networkDBAdaptor.getSummaryStats(null, null);
//    }
//
//    @Test
//    public void testGetNodes() throws Exception {
////        loadTestData();
//
//        // TESTING QUERIES
//        Query myQueryObject = new Query();
//        List<String> myXrefIds = new ArrayList<>();
////        myXrefIds.add("UniProt:P27466");
////        myXrefIds.add("CMK1");
////        myXrefIds.add("P27466");
////        myXrefIds.add("MIH1");
////        myXrefIds.add("CMK2");
//        myXrefIds.add("P02649");
//
//        myQueryObject.put(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), myXrefIds);
//        myQueryObject.put(NetworkDBAdaptor.NetworkQueryParams.JUMPS.key(), 2);
//        networkDBAdaptor.getNodes(myQueryObject, null);
//        System.out.println("The query took " + networkDBAdaptor.getNodes(myQueryObject, null).getDbTime() + " seconds.");
//    }
//
//    @Test
//    public void testBetweenness() throws Exception {
////        loadTestData();
//        networkDBAdaptor.betweenness(new Query("id", "CMK1"));
//
//    }
//
//    @Test
//    public void testClusteringCoefficient() throws Exception {
//        //loadTestData();
//
//        System.out.println(networkDBAdaptor.clusteringCoefficient(new Query("id", "PEP")).getResult().get(0));
//
////        Assert.assertEquals("Different clustering coefficient for \"PEP\": \n",
////                "#ID\tLOCATION\tCLUSTERING_COEFFICIENT\n"
////                        + "\"PEP\"\t\"cytosol\"\t\"0.95\"\n",
////                networkDBAdaptor.clusteringCoefficient(new Query("id", "PEP")).getResult().get(0));
//
//        System.out.println(networkDBAdaptor.clusteringCoefficient(new Query("id", "PEP,H2O")).getResult().get(0));
//
////        Assert.assertEquals("Different clustering coefficient for \"PEP,H2O\": \n",
////                "#ID\tLOCATION\tCLUSTERING_COEFFICIENT\n"
////                        + "\"PEP\"\t\"cytosol\"\t\"0.95\"\n"
////                        + "\"H2O\"\t\"mitochondrial matrix\"\t\"0.09\"\n"
////                        + "\"H2O\"\t\"cytosol\"\t\"NA\"\n"
////                        + "\"H2O\"\t\"peroxisomal matrix\"\t\"0.05\"\n"
////                        + "\"H2O\"\t\"endoplasmic reticulum lumen\"\t\"0.08\"\n",
////                networkDBAdaptor.clusteringCoefficient(new Query("id", "PEP,H2O")).getResult().get(0));
//    }
//
//    private void loadTestData() throws URISyntaxException, IOException, BioNetDBException {
//        BioPaxParser bioPaxParser = new BioPaxParser("L3");
//        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
//        Network network = bioPaxParser.parse(inputPath);
//        System.out.println("The file has been parsed.");
//
//        networkDBAdaptor.insert(network, null);
//        System.out.println("Data has been inserted in the database.");
//
//    }
//
//    @Test
//    public void testAddVariants() throws Exception {
//        ObjectMapper mapper = new ObjectMapper();
//        Path inputPath = Paths.get(getClass().getResource("/bionetdb_variants.json").toURI());
//        List<Variant> variants = mapper.readValue(inputPath.toFile(),
//                mapper.getTypeFactory().constructCollectionType(List.class, Variant.class));
//        for (Variant variant: variants) {
//            System.out.println(variant.toJson());
//            networkDBAdaptor.addVariants(Collections.singletonList(variant));
//        }
//    }
//
//    @Test
//    public void testInsertMetabolismHsapiens() throws Exception {
//        BioPaxParser bioPaxParser = new BioPaxParser("L3");
//        //Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
//        //Path inputPath = Paths.get("~/data150/neo4j/hsapiens.metabolism.biopax3");
//        Path inputPath = Paths.get("~/data150/neo4j/hsapiens.meiosis.biopax3");
//        Network network = bioPaxParser.parse(inputPath);
//        System.out.println("The file has been parsed.");
//
//        System.out.println("Inserting data...");
//        long startTime = System.currentTimeMillis();
//        networkDBAdaptor.insert(network, null);
//        long stopTime = System.currentTimeMillis();
//        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
//        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
//    }
//
//// TODO: update this code according to the refactoring Network code
////    @Test
////    public void testGetProteinXrefs() throws Exception {
////        StatementResult proteinXrefs = ((Neo4JNetworkDBAdaptor) networkDBAdaptor).getXrefs(PhysicalEntity.Type.PROTEIN.name());
////        int i = 0;
////        while (proteinXrefs.hasNext()) {
////            Record next = proteinXrefs.next();
////            Map<String, Object> map = next.asMap();
////            for (String key: map.keySet()) {
////                if (map.get(key) instanceof List) {
////                    System.out.println(">>>> List");
////                }
////                System.out.println(key + " -> " + map.get(key));
////            }
////            System.exit(-1);
////
////
////            System.out.println((++i) + ":\t" + next.get(0) + "\t" + next.get(1));
////            assert(next.get(2).asList().size() == next.get(3).asList().size());
////            int size = next.get(2).asList().size();
////            for (int j = 0; j < size; j++) {
////                System.out.println("\t" + next.get(2).asList().get(j) + " ---- " + next.get(3).asList().get(j));
////            }
////        }
////    }
}

//        Network networkToUpdate = new Network();
//        for (Relation relation : network.getRelations()) {
//            String xrefNodeId = relation..getDestId();
//            Entry entry = proteinMap.get(relation.getDestId());
//            Node proteinNode = new Node(0);
//            proteinNode.setId(relation.getOriginId());
//            networkToUpdate.setNode(proteinNode);
//
//            // Add XREF nodes for each protein
//            for (DbReferenceType dbReference: entry.getDbReference()) {
//                // ... create the Xref node
//                Xref xrefNode = new Xref(dbReference.getType(), "", dbReference.getId(), null);
//                networkToUpdate.setNode(xrefNode);
//
//                // ... and create relation protein -> protein variation annotation
//                Relation pXRel = new Relation(proteinNode.getId() + xrefNode.getId(), proteinNode.getId(),
//                        proteinNode.getType().toString(), xrefNode.getId(), xrefNode.getType().toString(),
//                        Relation.Type.XREF);
//                networkToUpdate.setRelationship(pXRel);
//            }
//
//            // Add PROTEIN_ANNOTATION node for each protein
//            // ... create the Xref node
//            String protAnnotNodeId = "ProteinAnnotation_uniprot:" + relation.getDestId();
//            Node protAnnotNode = new Node(protAnnotNodeId, null, Node.Type.PROTEIN_ANNOTATION);
//            ObjectMap update = new ObjectMap();
//            List<String> keywords = new ArrayList<>();
//            for (KeywordType keyword: entry.getKeyword()) {
//                keywords.add(keyword.getValue());
//            }
//            update.put("keywords", String.join(",", keywords));
//            protAnnotNode.addAttribute("_update", update);
//
//            network.addNode(protAnnotNode);
//
//            // ...and create relation consequence type -> protein variation annotation
//            Relation protAnnotRel = new Relation(relation.getOriginId() + protAnnotNode.getId(),
//                    relation.getOriginId(), Node.Type.PROTEIN.name(), protAnnotNode.getId(),
//                    protAnnotNode.getType().toString(), Relation.Type.ANNOTATION);
//            networkToUpdate.setRelationship(protAnnotRel);
//        }
//
//        // Add annotations to the network
//        System.out.println("Inserting data...");
//        long startTime = System.currentTimeMillis();
//        insert(networkToUpdate, null);
//        long stopTime = System.currentTimeMillis();
//        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
