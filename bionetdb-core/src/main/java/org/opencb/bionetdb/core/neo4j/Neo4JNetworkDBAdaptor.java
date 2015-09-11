package org.opencb.bionetdb.core.neo4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.NetworkDBException;
import org.opencb.bionetdb.core.models.*;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private static String DB_PATH;
    private GraphDatabaseService database;
    private boolean openedDB = false;

    public Neo4JNetworkDBAdaptor(String database) {
        this.DB_PATH = database;
        this.openedDB = true;
        this.database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(this.DB_PATH)
                .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
                .setConfig(GraphDatabaseSettings.relationship_auto_indexing, "true")
                .setConfig(GraphDatabaseSettings.node_keys_indexable, "id")
                .newGraphDatabase();

//        try (Transaction tx = this.database.beginTx()) {
//            IndexManager index = this.database.index();
//            Index<Node> id = index.forNodes("PhysicalEntity");
////        RelationshipIndex roles = index.forRelationships( "roles" );
//            tx.success();
//        }

        IndexDefinition indexDefinition;
        IndexDefinition indexDefinition2;
        try (Transaction tx = this.database.beginTx()) {
            Schema schema = this.database.schema();
            indexDefinition = schema.indexFor( DynamicLabel.label( "PhysicalEntity" ) )
                    .on( "id" )
                    .create();
            indexDefinition2 = schema.indexFor(DynamicLabel.label("PhysicalEntity"))
                    .on( "name" )
                    .create();
//            indexDefinition2 = schema.indexFor( DynamicLabel.label( "PhysicalEntity" ) )
//                    .on( "description" )
//                    .create();
            tx.success();
        }

//        try (Transaction tx = this.database.beginTx()) {
//            Schema schema = this.database.schema();
//            schema.awaitIndexOnline(indexDefinition, 10, TimeUnit.SECONDS);
//        }
    }

    private enum RelTypes implements RelationshipType {
        REACTANT,
        XREF,
        CONTROLLED,
        CONTROLLER,
        TISSUE,
        TIMESERIES
    }

    @Override
    public void addExpressionData(String tissue, String timeSeries, List<Expression> myExpression, QueryOptions options) {
        try (Transaction tx = this.database.beginTx()) {
            for (Expression exprElem : myExpression) {
                ObjectMap myProperty = new ObjectMap("id", exprElem.getId());
                Node xrefNode = getNode("Xref", myProperty);
                Node origin = null;

                // parsing options
                boolean addNodes = options.getBoolean("addNodes", false);

                // If the node does not already exist, it does not make sense inserting expression data.
                if (xrefNode == null && addNodes == true) {
                    // Create basic node with info
                    origin = createNode("PhysicalEntity", myProperty);
                    addXrefNode(origin, new Xref(null, null, exprElem.getId(), null));
                    xrefNode = getNode("Xref", myProperty);
                }
                if (xrefNode != null) {
                    if (origin == null)
                        origin = xrefNode.getSingleRelationship(RelTypes.XREF, Direction.INCOMING).getStartNode();
                    // Find or create tissueNode and the relationship
                    Node tissueNode = null;
                    for (Relationship relationShip : origin.getRelationships(RelTypes.TISSUE, Direction.OUTGOING)) {
                        if (relationShip.getEndNode().getProperty("tissue").equals(tissue)) {
                            tissueNode = relationShip.getEndNode();
                            break;
                        }
                    }
                    if (tissueNode == null) {
                        myProperty = new ObjectMap();
                        myProperty.put("tissue", tissue);
                        tissueNode = createNode("Tissue", myProperty);
                        addInteraction(origin, tissueNode, RelTypes.TISSUE);
                    }

                    // Find or create timeSeriesNode and the relationship
                    Node timeSeriesNode = null;
                    for (Relationship relationShip : tissueNode.getRelationships(RelTypes.TIMESERIES, Direction.OUTGOING)) {
                        if (relationShip.getEndNode().getProperty("timeseries").equals(timeSeries)) {
                            timeSeriesNode = relationShip.getEndNode();
                            break;
                        }
                    }
                    if (timeSeriesNode == null) {
                        myProperty = new ObjectMap();
                        myProperty.put("timeseries", timeSeries);
                        timeSeriesNode = createNode("TimeSeries", myProperty);
                        addInteraction(tissueNode, timeSeriesNode, RelTypes.TIMESERIES);
                    }

                    // Add or change the properties of the timeseries node in the database
                    if (exprElem.getExpression() != -1)
                        timeSeriesNode.setProperty("expression", exprElem.getExpression());
                    if (exprElem.getPvalue() != -1)
                        timeSeriesNode.setProperty("pvalue", exprElem.getPvalue());
                    if (exprElem.getOdds() != -1)
                        timeSeriesNode.setProperty("odds", exprElem.getOdds());
                    if (exprElem.getUpregulated() != -1)
                        timeSeriesNode.setProperty("upregulated", exprElem.getUpregulated());
                }
            }
            tx.success();
        }
    }

    private Node getNode (String label, ObjectMap properties) {
        IndexManager index = database.index();
//        System.out.println("Arrays.toString(index.nodeIndexNames()) = " + Arrays.toString(index.nodeIndexNames()));
        // TODO: Considering all the properties as String. This has to be changed.
        // TODO: At the moment, all the properties I'm inserting are strings. However, when issue #18 gets resolved, we should change the insertion of properties.
        // Cypher query
        StringBuilder myquery = new StringBuilder();
        myquery.append("MATCH (n:").append(label).append(") WHERE ");
        for (String key : properties.keySet()) {
            myquery.append("n.")
                    .append(key)
                    .append("= \"")
                    .append(properties.getString(key))
                    .append("\" AND ");
        }
        myquery.setLength(myquery.length() - 4); // Remove last AND
        myquery.append("RETURN n");

        Result result = this.database.execute(myquery.toString());
        if (result.hasNext()) {
            return (Node) result.next().get("n");
        } else {
            return null;
        }
    }

    private Node createNode (String label, ObjectMap properties) {
        // TODO: At the moment, all the properties Im inserting are strings. However, when issue #18 gets resolved, we should change the insertion of properties.

        Label mylabel = DynamicLabel.label(label);
        Node mynode = this.database.createNode(mylabel);
        for (String key : properties.keySet())
            //    mynode.setProperty(key, properties.get(key));
            mynode.setProperty(key, properties.getString(key));
        return mynode;
    }

    /**
     * @param label: Label of the node
     * @param properties: Map containing all the properties to be added. Key "id" must be among all the possible keys.
     * @return Node that has been created.
     */
    private Node getOrCreateNode(String label, ObjectMap properties) {
        Node mynode = getNode(label, properties);
        if (mynode == null) {
            mynode = createNode(label, properties);
            // addXrefNode(mynode, new Xref(null, null, properties.get("id").toString(), null));
        }
        return mynode;
    }

    /**
     * The function will create an interaction between two nodes if it is not already created.
     * @param origin Node from which we want to create the interaction
     * @param destination Destination node
     * @param relationType Type of relationship between nodes
     */
    private void addInteraction(Node origin, Node destination, RelTypes relationType) {
        if (origin.hasRelationship(relationType, Direction.OUTGOING)) {
            for (Relationship r : origin.getRelationships(relationType, Direction.OUTGOING)) {
                if (r.getEndNode().equals(destination)) {
                    return;
                }
            }
        }
        origin.createRelationshipTo(destination, relationType);
    }

    /**
     * Insert an entire network into the Neo4J database
     * @param network Object containing all the nodes and interactions
     * @param queryOptions
     */
    @Override
    public void insert(Network network, QueryOptions queryOptions) throws NetworkDBException {
        this.insertPhysicalEntities(network.getPhysicalEntities(), queryOptions);
        this.insertInteractions(network.getInteractions(), queryOptions);
    }

    /**
     * Insert all the interactions into the Neo4J database
     * @param interactionList List containing all the interactions to be inserted in the database
     * @param queryOptions
     */
    private void insertInteractions(List<Interaction> interactionList, QueryOptions queryOptions) {

        // 1. Insert all interactions as nodes
        try (Transaction tx = this.database.beginTx()) {
            for (Interaction i : interactionList) {
                ObjectMap myProperties = new ObjectMap();
                myProperties.put("id", i.getId());
                Node mynode =  getOrCreateNode("Interaction", myProperties);
                if (i.getName() != null) {
                    mynode.setProperty("name", i.getName());
                }
                if (i.getId() != null) {
                    mynode.setProperty("id", i.getId());
                }
                if (i.getDescription() != null) {
                    mynode.setProperty("description", i.getDescription());
                }
            }
            tx.success();
        }

        // 2. Insert the interactions
        try (Transaction tx = this.database.beginTx()) {
            Label interactionLabel = DynamicLabel.label("Interaction");
            Label pEntityLabel = DynamicLabel.label("PhysicalEntity");
            Node r;
            for (Interaction i : interactionList) {
                switch(i.getType()) {
                    case REACTION:
                        // Left & right
                        Reaction myreaction = (Reaction) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        for (String myID : myreaction.getReactants()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addInteraction(n, r, RelTypes.REACTANT);
                        }

                        for (String myID : myreaction.getProducts()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addInteraction(r, n, RelTypes.REACTANT);
                        }
                        break;
                    case CATALYSIS:
                        Catalysis catalysis = (Catalysis) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        // Left reactions (controllers)
                        for (String myID : catalysis.getControllers()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addInteraction(n, r, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        for (String myID : catalysis.getControlledProcesses()) {
                            Node n = this.database.findNode(interactionLabel, "id", myID);
                            addInteraction(r, n, RelTypes.CONTROLLED);
                        }
                        break;
                    case REGULATION:
                        Regulation regulation = (Regulation) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        // Left reactions (controllers)
                        for (String myID : regulation.getControllers()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addInteraction(n, r, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        for (String myID : regulation.getControlledProcesses()) {
                            Node n = this.database.findNode(interactionLabel, "id", myID);
                            addInteraction(r, n, RelTypes.CONTROLLED);
                        }
                        break;
                    default:
                        break;
                }
            }
            tx.success();
        }
    }

    /**
     * Insert physical entities into the Neo4J database
     * @param physicalEntityList List containing all the physical entities to be inserted in the database
     * @param queryOptions
     */
    private void insertPhysicalEntities(List<PhysicalEntity> physicalEntityList, QueryOptions queryOptions) {
        try ( Transaction tx = this.database.beginTx() ) {
            for (PhysicalEntity p : physicalEntityList) {
                // Insert all the Physical entity nodes
                ObjectMap myProperties = new ObjectMap();
                myProperties.put("id", p.getId());
                myProperties.put("name", p.getName());
                myProperties.put("description", p.getDescription());
                Node n = getOrCreateNode("PhysicalEntity", myProperties);
                addXrefNode(n, new Xref(null, null, p.getId(), null));
            }
            tx.success();
        }
    }

    /**
     * Create xref annotation and creates the link from node to xref_node
     * @param node Main node from which we are going to add the annotation
     * @param xref Xref object containing information to be added in the database
     */
    private void addXrefNode(Node node, Xref xref) {
        ObjectMap myProperties = new ObjectMap();
        if (xref.getSource() != null)
            myProperties.put("source", xref.getSource());
        if (xref.getId() != null)
            myProperties.put("id", xref.getId());
        if (xref.getSourceVersion() != null)
            myProperties.put("sourceVersion", xref.getSourceVersion());
        if (xref.getIdVersion() != null)
            myProperties.put("idVersion", xref.getIdVersion());

        // TODO: Problems: This will check first if there exists an Xref with all those properties.
        // TODO: If there exist an Xref with the same id, but different properties, it will create another node.
        Node xref_node = getOrCreateNode("Xref", myProperties);

        if (!xref_node.hasRelationship(RelTypes.XREF, Direction.INCOMING))
            node.createRelationshipTo(xref_node, RelTypes.XREF);
    }

    /**
     * Method to annotate Xrefs in the database
     * @param nodeID ID of the node we want to annotate
     * @param xref_list List containing all the Xref annotations to be added in the database
     */
    @Override
    public void addXrefs(String nodeID, List<Xref> xref_list) {
        ObjectMap myProperties = new ObjectMap("id", nodeID);
        try ( Transaction tx = this.database.beginTx()) {
            Node xrefNode = getNode("Xref", myProperties);
            if (xrefNode != null) {
                //Look for the physical entity to which the xref is associated with
                Node n = xrefNode.getSingleRelationship(RelTypes.XREF, Direction.INCOMING).getStartNode();
                for (Xref x : xref_list) {
                    addXrefNode(n, x);
                }
            } else {
                // TODO: Exception telling that the node "nodeID" does not exist, so we cannot annotate.
            }
            tx.success();
        }
    }

    @Override
    public QueryResult get(Query query, QueryOptions queryOptions) {
        Result result = this.database.execute(query.get("query").toString());
        return new QueryResult(result.columnAs(queryOptions.get("columnsAs").toString()).next().toString());
    }

    @Override
    public QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions) {
        return null;
    }

    private int getTotalNodes() {
        return Integer.parseInt(this.database.execute("START n=node(*) RETURN count(n)").columnAs("count(n)").next().toString());
    }

    private int getTotalRelationships() {
        return Integer.parseInt(this.database.execute("START n=relationship(*) RETURN count(n)").columnAs("count(n)").next().toString());
    }

    @Override
    public QueryResult getStats(Query query, QueryOptions queryOptions) {
        long startTime = System.currentTimeMillis();
        List<Integer> myoutput = new ArrayList<>(2);
        myoutput.add(getTotalNodes());
        myoutput.add(getTotalRelationships());
        int time = (int) (System.currentTimeMillis() - startTime);
        return new QueryResult<>("stats", time, 2, 2, null, null, myoutput);
    }

    public boolean isOpened() throws IOException {
        if (this.openedDB == true)
            return true;
        else
            throw new IOException("Database is closed.");
    }

    public void close() {
        this.database.shutdown();
    }

}
