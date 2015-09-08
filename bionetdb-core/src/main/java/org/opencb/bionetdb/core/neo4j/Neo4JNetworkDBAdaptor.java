package org.opencb.bionetdb.core.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.models.*;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

import java.io.IOException;
import java.util.List;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private static String DB_PATH;
    private GraphDatabaseService database;
    private boolean openedDB = false;

    public Neo4JNetworkDBAdaptor(String database) {
        this.DB_PATH = database;
        this.database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( this.DB_PATH ).
                setConfig( GraphDatabaseSettings.node_keys_indexable, "id" ).
                setConfig(GraphDatabaseSettings.node_auto_indexing, "true").
                newGraphDatabase();
        this.openedDB = true;
    }

    private enum RelTypes implements RelationshipType
    {
        REACTANT,
        XREF,
        CONTROLLED,
        CONTROLLER,
        TISSUE,
        TIMESERIES
    }

    @Override
    public void addExpressionData(String tissue, String timeseries, List<Expression> myExpression) {
        for (Expression exprElem : myExpression) {
            Node xref_node = getNode("Xref", "id", exprElem.getId());
            Node origin = xref_node.getSingleRelationship(RelTypes.XREF, Direction.INCOMING).getStartNode();

            // Find or create tissueNode and the relationship
            Node tissueNode = null;
            for (Relationship relationShip : origin.getRelationships(RelTypes.TISSUE, Direction.OUTGOING)) {
                if (relationShip.getEndNode().getProperty("tissue").equals(tissue)) {
                    tissueNode = relationShip.getEndNode();
                    break;
                }
            }
            if (tissueNode == null) {
                tissueNode = createNode("Tissue");
                tissueNode.setProperty("tissue", exprElem.getId());
                addInteraction(origin, tissueNode, RelTypes.TISSUE);
            }

            // Find or create timeSeriesNode and the relationship
            Node timeSeriesNode = null;
            for (Relationship relationShip : tissueNode.getRelationships(RelTypes.TIMESERIES, Direction.OUTGOING)) {
                if (relationShip.getEndNode().getProperty("timeseries").equals(timeseries)) {
                    timeSeriesNode = relationShip.getEndNode();
                    break;
                }
            }
            if (timeSeriesNode == null) {
                timeSeriesNode = createNode("TimeSeries");
                timeSeriesNode.setProperty("timeseries", exprElem.getId());
                addInteraction(tissueNode, timeSeriesNode, RelTypes.TISSUE);
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

    /**
     * The function will look for a node in the database. If it does not exist, it will create it.
     * @param label Label of the node
     * @param query Query parameters used to look for the node
     * @param queryOptions
     * @return Returns a node.
     */
    // TODO: Improve query & queryOptions usage
    private Node getOrCreateNode(String label, Query query, QueryOptions queryOptions) {

        Node result = getNode(label, "id", query.get("id"));
        if (result == null) {
            result = createNode(label);
        }
        return result;
    }

    private Node getNode (String label, String key, Object value) {
        Label mylabel = DynamicLabel.label(label);
        return this.database.findNode(mylabel, key, value);
    }

    private Node createNode (String label) {
        Label mylabel = DynamicLabel.label(label);
        return this.database.createNode(mylabel);
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
    public void insert(Network network, QueryOptions queryOptions) {

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
                Node mynode =  getOrCreateNode("Interaction", new Query("id", i.getId()), null);
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
                Node mynode = getOrCreateNode("PhysicalEntity", new Query("id", p.getId()), null);
                mynode.setProperty("id", p.getId());
                mynode.setProperty("name", p.getName());
                mynode.setProperty("description", p.getDescription());
                addXrefNode(mynode, new Xref(null, null, p.getId(), null));
            }
            tx.success();
        }
    }

    /**
     * Method to annotate (create or modify) Xref elements into the Neo4J database
     * @param node Main node from which we are going to add the annotation
     * @param xref Xref object containing information to be added in the database
     */
    private void addXrefNode(Node node, Xref xref) {

        try ( Transaction tx = this.database.beginTx() ) {

            Node xref_node = getOrCreateNode("Xref", new Query("id", xref.getId()), null);

            if (xref.getSource() != null) xref_node.setProperty("source", xref.getSource());
            if (xref.getId() != null) xref_node.setProperty("id", xref.getId());
            if (xref.getSourceVersion() != null) xref_node.setProperty("sourceVersion", xref.getSourceVersion());
            if (xref.getIdVersion() != null) xref_node.setProperty("idVersion", xref.getIdVersion());

            if (!xref_node.hasRelationship(RelTypes.XREF, Direction.INCOMING))
                node.createRelationshipTo(xref_node, RelTypes.XREF);

            tx.success();
        }
    }

    /**
     * Method to annotate Xrefs in the database
     * @param nodeID ID of the node we want to annotate
     * @param xref_list List containing all the Xref annotations to be added in the database
     */
    @Override
    public void addXrefs(String nodeID, List<Xref> xref_list) {

        Label xrefLabel    = DynamicLabel.label("Xref");
        Label pEntityLabel = DynamicLabel.label("PhysicalEntity");
        try ( Transaction tx = this.database.beginTx() ) {
            // 1. We look for the ID in physical entities directly
            Node n = this.database.findNode(pEntityLabel, "id", nodeID);

            // 2. Otherwise, we look in other xrefs
            if (n == null) {
                Node xrefNode = this.database.findNode(xrefLabel, "id", nodeID);
                if (xrefNode != null) {
                    //Look for the physical entity to which the xref is associated with
                    n = xrefNode.getSingleRelationship(RelTypes.XREF, Direction.INCOMING).getStartNode();
                }
            }

            for (Xref x : xref_list) {
                addXrefNode(n, x);
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

    @Override
    public QueryResult stats(Query query, QueryOptions queryOptions) {
        String nnodes = this.database.execute("START n=node(*) RETURN count(n)").columnAs("count(n)").next().toString();
        String nrelat = this.database.execute("START n=relationship(*) RETURN count(n)").columnAs("count(n)").next().toString();
        return new QueryResult("Nodes: " + nnodes + "\tRelationships: " + nrelat);
    }

    public boolean isOpened() throws IOException {
        if (this.openedDB == true)
            return true;
        else
            throw new IOException("Database is closed.");
    }


    @Override
    public void close() throws Exception {

        this.database.shutdown();

    }

}
