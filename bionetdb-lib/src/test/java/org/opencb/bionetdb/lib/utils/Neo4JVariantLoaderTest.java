package org.opencb.bionetdb.lib.utils;

import org.junit.Test;

public class Neo4JVariantLoaderTest {

    @Test
    public void test1() {
//        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(new File("/home/jtarraga/soft/neo4j/data/databases/graph.db")).build();
//        GraphDatabaseService graphDb = managementService.database("neo4j");
//
////        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("/home/jtarraga/soft/neo4j/data/databases/graph.db") );
//
//        Label aaLabel = Label.label("AA");
//        Label geneLabel = Label.label("GENE");
//
//        try (Transaction tx = graphDb.beginTx()) {
//
//            //Node node = graphDb.createNode(aaLabel);
//            //node.setProperty("name", "toto");
//
//            int count = 0;
//
////            String geneId = "ENSG00000139567";
////            ResourceIterator<Node> nodes = graphDb.findNodes(geneLabel, "id", geneId);//, "name", "toto");
////            while (nodes.hasNext()) {
////                System.out.println(nodes.next().getAllProperties());
////
////                if (++count > 10) {
////                    break;
////                }
////            }
//
//            //tx.success();
//        }
////        graphDb.shutdown();
    }

    @Test
    public void testAllXref() {
//        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(new File("/home/jtarraga/soft/neo4j/data/databases/graph.db")).build();
//        GraphDatabaseService graphDb = managementService.database("neo4j");
//
//        Label label = Label.label("XREF");
//
//        int count = 0;
//        int maxCount = 10;
//
//        try (Transaction tx = graphDb.beginTx()) {
//
////            ResourceIterator<Node> nodes = graphDb.findNodes(label);
////            while (nodes.hasNext()) {
////                Node next = nodes.next();
////                System.out.println(next.getId() + " -> " + next.getAllProperties());
////
////                if (++count > maxCount) {
////                    break;
////                }
////            }
//
//            //tx.success();
//        }
////        graphDb.shutdown();
    }

    @Test
    public void testCreateRelationship() {
//        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(new File("/home/jtarraga/soft/neo4j/data/databases/graph.db")).build();
//        GraphDatabaseService graphDb = managementService.database("neo4j");
//
//        Label aaLabel = Label.label("AA");
//        Label bbLabel = Label.label("BB");
//
//        String relationshipName = aaLabel.toString() + "-" + bbLabel.toString();
//
//        try (Transaction tx = graphDb.beginTx()) {
//
////            Node aaNode = graphDb.createNode(aaLabel);
////            aaNode.setProperty("id", 1);
////            aaNode.setProperty("name", "name-a-1");
////
////            Node bbNode = graphDb.createNode(bbLabel);
////            bbNode.setProperty("id", 2);
////            bbNode.setProperty("name", "name-b-2");
//
////            Relationship aaBB = aaNode.createRelationshipTo(bbNode, RelationshipType.withName(relationshipName));
////            aaBB.setProperty("score", 1.22);
//
//            //tx.success();
//        }
////        graphDb.shutdown();
    }
}