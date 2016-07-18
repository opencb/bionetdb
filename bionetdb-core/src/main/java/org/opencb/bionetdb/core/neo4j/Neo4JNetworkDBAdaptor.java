package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;

import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private String databasePath;
    private String databaseURI;

    @Deprecated
//    private GraphDatabaseService database;
    private boolean openedDB = false;

    private Driver driver;
    private Session session;

    private BioNetDBConfiguration configuration;

    //    private enum RelTypes implements RelationshipType {
    private enum RelTypes {
        REACTANT,
        PRODUCT,
        XREF,
        CONTROLLED,
        CONTROLLER,
        TISSUE,
        TIMESERIES,
        ONTOLOGY,
        COMPONENTOFCOMPLEX,
        CELLULARLOCATION,
        CEL_ONTOLOGY
    }

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration) throws BioNetDBException {
        this(database, configuration, false);
    }

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration, boolean createIndex) throws BioNetDBException {
        this.configuration = configuration;

        DatabaseConfiguration databaseConfiguration = getDatabaseConfiguration(database);

        if (databaseConfiguration == null) {
            throw new BioNetDBException("No database found for database: '" + database + "'");
        }
        this.databasePath = databaseConfiguration.getPath();
        this.databaseURI = databaseConfiguration.getHost() + ":" + databaseConfiguration.getPort();
        this.openedDB = true;
//        this.database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(this.databasePath)).newGraphDatabase();

        driver = GraphDatabase.driver("bolt://" + this.databaseURI, AuthTokens.basic("neo4j", "neo4j"));
        session = driver.session();

        registerShutdownHook(this.driver, this.session);

        // this must be last line, it needs 'database' to be created
        if (createIndex) {
            createIndexes();
        }
    }

    private void registerShutdownHook(final Driver driver, final Session session) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
//                database.shutdown();
                session.close();
                driver.close();
            }
        });
    }

    private DatabaseConfiguration getDatabaseConfiguration(String database) {
        DatabaseConfiguration databaseConfiguration;
        if (database != null && !database.isEmpty()) {
            databaseConfiguration = configuration.findDatabase(database);
        } else {
            databaseConfiguration = configuration.findDatabase();
        }

        return databaseConfiguration;
    }

    private void createIndexes() {
        if (this.session != null) {
            try (Transaction tx = this.session.beginTransaction()) {
                tx.run("CREATE INDEX ON :PhysicalEntity(id)");
                tx.run("CREATE INDEX ON :PhysicalEntity(name)");

                tx.run("CREATE INDEX ON :Interaction(id)");
                tx.run("CREATE INDEX ON :Interaction(name)");

                tx.run("CREATE INDEX ON :Xref(id)");
                tx.run("CREATE INDEX ON :Tissue(tissue)");
                tx.run("CREATE INDEX ON :TimeSeries(timeseries)");

                tx.success();
            }
        }
    }

    /**
     * Insert an entire network into the Neo4J database.
     *
     * @param network Object containing all the nodes and interactions
     * @param queryOptions Optional params
     */
    @Override
    public void insert(Network network, QueryOptions queryOptions) throws BioNetDBException {
        this.insertPhysicalEntities(network.getPhysicalEntities(), queryOptions);
        this.insertInteractions(network.getInteractions(), queryOptions);
    }

    /**
     * Method to annotate Xrefs in the database.
     *
     * @param nodeID ID of the node we want to annotate
     * @param xrefList List containing all the Xref annotations to be added in the database
     */
    @Override
    public void addXrefs(String nodeID, List<Xref> xrefList) throws BioNetDBException {
/*        try (Transaction tx = this.session.beginTransaction()) {
            Node xrefNode = getNode("Xref", new ObjectMap("id", nodeID));
            if (xrefNode != null) {
                // Look for the physical entity to which the xref is associated with
                for (Relationship relationship : xrefNode.getRelationships(RelTypes.XREF, Direction.INCOMING)) {
                    Node n = relationship.getStartNode();
                    for (Xref x : xrefList) {
                        addXrefNode(n, x);
                    }
                }
            } else {
                throw new BioNetDBException("The node to be annotated does not exist in the database.");
            }
            tx.success();
        }
*/    }

    @Override
    public void addExpressionData(String tissue, String timeSeries, List<Expression> myExpression, QueryOptions options) {
 /*       try (Transaction tx = this.database.beginTx()) {
            for (Expression exprElem : myExpression) {
                ObjectMap myProperty = new ObjectMap("id", exprElem.getId());
                Node xrefNode = getNode("Xref", myProperty);
                // parsing options
                boolean addNodes = options.getBoolean("addNodes", false);

                // If the node does not already exist, it does not make sense inserting expression data.
                if (xrefNode == null && addNodes) {
                    // Create basic node with info
                    Node origin = createNode("PhysicalEntity", myProperty);
                    addXrefNode(origin, new Xref(null, null, exprElem.getId(), null));
                    xrefNode = getNode("Xref", myProperty);
                }

                if (xrefNode != null) {
                    Node origin;
                    for (Relationship relationship : xrefNode.getRelationships(RelTypes.XREF, Direction.INCOMING)) {
                        origin = relationship.getStartNode();

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
                            addRelationship(origin, tissueNode, RelTypes.TISSUE);
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
                            addRelationship(tissueNode, timeSeriesNode, RelTypes.TIMESERIES);
                        }

                        // Add or change the properties of the timeseries node in the database
                        if (exprElem.getExpression() != -1) {
                            timeSeriesNode.setProperty("expression", exprElem.getExpression());
                        }
                        if (exprElem.getPvalue() != -1) {
                            timeSeriesNode.setProperty("pvalue", exprElem.getPvalue());
                        }
                        if (exprElem.getOdds() != -1) {
                            timeSeriesNode.setProperty("odds", exprElem.getOdds());
                        }
                        if (exprElem.getUpregulated() != -1) {
                            timeSeriesNode.setProperty("upregulated", exprElem.getUpregulated());
                        }
                    }
                }
            }
            tx.success();
        }
*/    }


    private ObjectMap parsePhysicalEntity(PhysicalEntity myPhysicalEntity) {
        ObjectMap myOutput = new ObjectMap("id", myPhysicalEntity.getId());
        if (myPhysicalEntity.getName() != null) {
            myOutput.put("name", myPhysicalEntity.getName());
        }
        if (myPhysicalEntity.getDescription() != null) {
            myOutput.put("description", myPhysicalEntity.getDescription());
        }
        if (myPhysicalEntity.getType() != null) {
            myOutput.put("type", myPhysicalEntity.getType());
        }
        if (myPhysicalEntity.getSource().size() > 0) {
            myOutput.put("source", myPhysicalEntity.getSource());
        }
        return myOutput;
    }

    /***
     * Insert all the elements present in the Ontology object into an ObjectMap object.
     * @param myOntology Ontology object
     * @return ObjectMap object containing the values present in myOntology
     */
    private ObjectMap parseOntology(Ontology myOntology) {
        ObjectMap myOutput = new ObjectMap("id", myOntology.getId());
        if (myOntology.getSource() != null) {
            myOutput.put("source", myOntology.getSource());
        }
        if (myOntology.getDescription() != null) {
            myOutput.put("description", myOntology.getDescription());
        }
        if (myOntology.getName() != null) {
            myOutput.put("name", myOntology.getName());
        }
        if (myOntology.getIdVersion() != null) {
            myOutput.put("idVersion", myOntology.getIdVersion());
        }
        if (myOntology.getSourceVersion() != null) {
            myOutput.put("sourceVersion", myOntology.getSourceVersion());
        }
        return myOutput;
    }

    /***
     * Insert all the elements present in the CellularLocation object into an ObjectMap object.
     *
     * @param myCellularLocation Cellular Location Object
     * @return ObjectMap object containing the values present in myCellularLocation
     */
    private ObjectMap parseCellularLocation(CellularLocation myCellularLocation) {
        ObjectMap myOutput = new ObjectMap("id", myCellularLocation.getName());
        List<Ontology> myOntologies = myCellularLocation.getOntologies();
        List<ObjectMap> allOntologies = new ArrayList<>();
        if (myOntologies.size() > 0) {
            for (Ontology myOntology : myOntologies) {
                allOntologies.add(parseOntology(myOntology));
            }
        }
        myOutput.put("ontologies", allOntologies);
        return myOutput;
    }

    /***
     * Insert all the elements present in the CellularLocation object into an ObjectMap object.
     *
     * @param xref Xref Object
     * @return ObjectMap object containing the values present in myCellularLocation
     */
    private ObjectMap parseXref(Xref xref) {
        ObjectMap myProperties = new ObjectMap();
        if (xref.getSource() != null) {
            myProperties.put("source", xref.getSource());
        }
        if (xref.getId() != null) {
            myProperties.put("id", xref.getId());
        }
        if (xref.getSourceVersion() != null) {
            myProperties.put("sourceVersion", xref.getSourceVersion());
        }
        if (xref.getIdVersion() != null) {
            myProperties.put("idVersion", xref.getIdVersion());
        }
        return myProperties;
    }

    private ObjectMap parseInteraction(Interaction interaction) {
        ObjectMap myProperties = new ObjectMap();
        if (interaction.getName() != null) {
            myProperties.put("name", interaction.getName());
        }
        if (interaction.getId() != null) {
            myProperties.put("id", interaction.getId());
        }
        if (interaction.getDescription() != null) {
            myProperties.put("description", interaction.getDescription());
        }
        return myProperties;
    }

    private String concatenateLabels(Value labels) {
        return labels.toString().replace("\", \"", ":").replaceAll("[\"\\[\\]]", "");
    }

    /**
     * Insert physical entities into the Neo4J database.
     *
     * @param physicalEntityList List containing all the physical entities to be inserted in the database
     * @param queryOptions Additional params for the query
     */
    private void insertPhysicalEntities(List<PhysicalEntity> physicalEntityList, QueryOptions queryOptions) throws BioNetDBException {
        try (Transaction tx = this.session.beginTransaction()) {
            // 1. Insert the Physical Entities and the basic nodes they are connected to
            for (PhysicalEntity p : physicalEntityList) {
                String peLabel = "PhysicalEntity:" + p.getType();
                StatementResult n = getOrCreateNode(tx, peLabel, parsePhysicalEntity(p));
                String physicalEntityID = n.peek().get("ID").toString();
                System.out.println(concatenateLabels(n.peek().get("LABELS")));

                // 1.1. Insert the ontologies
                for (Ontology o : p.getOntologies()) {
                    StatementResult ont = getOrCreateNode(tx, "Ontology", parseOntology(o));
                    addRelationship(tx, peLabel, "Ontology", physicalEntityID,
                            ont.peek().get("ID").toString(), RelTypes.ONTOLOGY);
                }

                // 1.2. Insert the cellular locations
                for (CellularLocation c : p.getCellularLocation()) {
                    StatementResult cellLoc = getOrCreateCellularLocationNode(tx, parseCellularLocation(c));
                    addRelationship(tx, peLabel, "CellularLocation", physicalEntityID,
                            cellLoc.peek().get("ID").toString(), RelTypes.CELLULARLOCATION);
                }

                // 1.3. Insert the Xrefs
                for (Xref xref : p.getXrefs()) {
                    StatementResult xr = getOrCreateNode(tx, "Xref", parseXref(xref));
                    addRelationship(tx, peLabel, "Xref", physicalEntityID,
                            xr.peek().get("ID").toString(), RelTypes.XREF);
                }
            }
            tx.success();
        }
        try (Transaction tx = this.session.beginTransaction()) {
            // 2. Insert the existing relationships between Physical Entities
            for (PhysicalEntity p : physicalEntityList) {
                if (p.getComponentOfComplex().size() > 0) {
                    StatementResult peNode = getNode(tx, "PhysicalEntity", parsePhysicalEntity(p));
                    if (peNode == null) {
                        throw new BioNetDBException("PhysicalEntity \"" + p.getId()
                                + "\" is not properly inserted in the database.");
                    }
                    for (String complexID : p.getComponentOfComplex()) {
                        StatementResult complexNode = getNode(tx, "PhysicalEntity", new ObjectMap("id", complexID));
                        if (complexNode == null) {
                            throw new BioNetDBException("PhysicalEntity:Complex \"" + complexID
                                    + "\": is not properly inserted");
                        }
                        if (complexNode.peek().get("LABELS").get(1).toString().equals("\"COMPLEX\"")) {
                            String peLabel = "PhysicalEntity:" + p.getType();
                            addRelationship(tx, peLabel, "PhysicalEntity:COMPLEX",
                                    peNode.peek().get("ID").toString(), complexNode.peek().get("ID").toString(),
                                    RelTypes.COMPONENTOFCOMPLEX);
                        } else {
                            throw new BioNetDBException("Relationship 'COMPONENTOFCOMPLEX' cannot be created "
                                    + "because the destiny node is of type \""
                                    + complexNode.peek().get("LABELS").toString()
                                    + "\" Check Physical Entity \"" + complexID + "\"");
                        }
                    }
                }
            }
            tx.success();
        }
    }

    /**
     * Insert all the interactions into the Neo4J database.
     *
     * @param interactionList List containing all the interactions to be inserted in the database
     * @param queryOptions Additional params for the query
     */
    private void insertInteractions(List<Interaction> interactionList, QueryOptions queryOptions) {

        // 1. Insert all interactions as nodes
        try (Transaction tx = this.session.beginTransaction()) {
            for (Interaction i : interactionList) {
                String interactionLabel = "Interaction:" + i.getType();
                StatementResult interaction = getOrCreateNode(tx, interactionLabel, parseInteraction(i));
            }
            tx.success();
        }
/*
        // 2. Insert the interactions
        try (Transaction tx = this.session.beginTransaction()) {
            for (Interaction i : interactionList) {
                String interactionLabel = "Interaction:" + i.getType();
                StatementResult interaction = getNode(tx, interactionLabel, new ObjectMap("id", i.getId()));
                String interactionID = interaction.peek().get("ID").toString();

                switch (i.getType()) {
                    case REACTION:
                        Reaction myreaction = (Reaction) i;
                        for (String myId : myreaction.getReactants()) {
                            StatementResult reactant = getNode(tx, "", new ObjectMap("id", myId));
                            addRelationship(tx, oriLabel, interactionLabel, reactant.peek().get("ID").toString(), interactionID,
                            RelTypes.REACTANT);
                        }

                        for (String myId : myreaction.getProducts()) {
                            addRelationship(tx, oriLabel, destLabel, oriID, destID, type);
                        }
                        break;
                    }

                }
            }
*/
/*

            Label interactionLabel = Label.label("Interaction");
            Label pEntityLabel = Label.label("PhysicalEntity");
            Node r;
            for (Interaction i : interactionList) {
                switch(i.getType()) {
                    case REACTION:
                        // Left & right
                        Reaction myreaction = (Reaction) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        for (String myID : myreaction.getReactants()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addRelationship(n, r, RelTypes.REACTANT);
                        }

                        for (String myID : myreaction.getProducts()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addRelationship(r, n, RelTypes.REACTANT);
                        }
                        break;
                    case CATALYSIS:
                        Catalysis catalysis = (Catalysis) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        // Left reactions (controllers)
                        for (String myID : catalysis.getControllers()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addRelationship(n, r, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        for (String myID : catalysis.getControlledProcesses()) {
                            Node n = this.database.findNode(interactionLabel, "id", myID);
                            addRelationship(r, n, RelTypes.CONTROLLED);
                        }
                        break;
                    case REGULATION:
                        Regulation regulation = (Regulation) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        // Left reactions (controllers)
                        for (String myID : regulation.getControllers()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            addRelationship(n, r, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        for (String myID : regulation.getControlledProcesses()) {
                            Node n = this.database.findNode(interactionLabel, "id", myID);
                            addRelationship(r, n, RelTypes.CONTROLLED);
                        }
                        break;
                    default:
                        break;
                }
            }
            tx.success();
        }
*/    }

    private StatementResult getNode(Transaction tx, String label, ObjectMap properties) {
        // Gathering properties of the node to create a cypher string with them
        List<String> props = new ArrayList<>();
        for (String key : properties.keySet()) {
            props.add(key + ":\"" + properties.getString(key) + "\"");
        }
        String propsJoined = "{" + String.join(",", props) + "}";
        // Getting the desired node
        return tx.run("MATCH (n:" + label + " " + propsJoined + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS ");
    }

    private StatementResult createNode(Transaction tx, String label, ObjectMap properties) {
        // Gathering properties of the node to create a cypher string with them
        List<String> props = new ArrayList<>();
        for (String key : properties.keySet()) {
            props.add(key + ":\"" + properties.getString(key) + "\"");
        }
        String propsJoined = "{" + String.join(",", props) + "}";
        // Creating the desired node
        return tx.run("CREATE (n:" + label + " " + propsJoined + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS ");
    }

    /**
     * @param label: Label of the node
     * @param properties: Map containing all the properties to be added. Key "id" must be among all the possible keys.
     * @return Node that has been created.
     */
    private StatementResult getOrCreateNode(Transaction tx, String label, ObjectMap properties) {
        // Gathering properties of the node to create a cypher string with them
        List<String> props = new ArrayList<>();
        for (String key : properties.keySet()) {
            props.add(key + ":\"" + properties.getString(key) + "\"");
        }
        String propsJoined = "{" + String.join(",", props) + "}";
        // Getting the desired node or creating it if it does not exists
        return tx.run("MERGE (n:" + label + " " + propsJoined + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS ");
    }

    /**
     * Checks if the cellular location given in properties is already in database. If not, it creates it and
     * returns the node.
     * @param properties
     * @return
     */
    private StatementResult getOrCreateCellularLocationNode(Transaction tx, ObjectMap properties) {
        StatementResult cellLoc = getOrCreateNode(tx, "CellularLocation", new ObjectMap("id", properties.get("id")));
        // gets or creates ontology node
        if (properties.containsKey("ontologies")) {
            for (ObjectMap myOntology : (List<ObjectMap>) properties.get("ontologies")) {
                StatementResult ont = getOrCreateNode(tx, "Ontology", myOntology);
                addRelationship(tx, "CellularLocation", "Ontology", cellLoc.peek().get("ID").toString(),
                        ont.peek().get("ID").toString(), RelTypes.CEL_ONTOLOGY);
            }
        }
        return cellLoc;
    }

    /**
     * The function will create an interaction between two nodes if the relation does not exist.
     *
     * @param tx Transaction
     * @param originID Node ID from which we want to create the interaction
     * @param destinationID Destination node ID
     * @param relationType Type of relationship between nodes
     */
    private void addRelationship(Transaction tx, String labelOri, String labelDest, String originID,
                                 String destinationID, RelTypes relationType) {
        tx.run("MATCH (o:" + labelOri + ") WHERE ID(o) = " + originID
                + " MATCH (d:" + labelDest + ") WHERE ID(d) = " + destinationID
                + " MERGE (o)-[:" + relationType + "]->(d)");
    }

    /**
     * This method will be called every time we consider that two existing nodes are the same and should be merged.
     *
     * @param node1
     * @param node2
     */
    // TODO: Maybe we would have to add the type of nodes we want to merge... Now it is done considering they are PE
    private void mergeNodes(Node node1, Node node2) {
/*        Node myNewNode = null;
        // TODO: 1. Merge the basic information from both nodes into this one.
        // 2. Destroy the relationships present in both nodes and apply them to the new node.
        // TODO: Check expression data
        // TODO: What would happen if two different nodes have different expression data....??
        // TODO: What if both of them have expression for the same tissues, but for different timeseries??
        // Expression data
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.TISSUE, Direction.OUTGOING)) {
            addRelationship(myNewNode, nodeAux, RelTypes.TISSUE);
        }
        // Ontologies
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.ONTOLOGY, Direction.OUTGOING)) {
            addRelationship(myNewNode, nodeAux, RelTypes.ONTOLOGY);
        }
        // Xrefs
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.XREF, Direction.OUTGOING)) {
            addRelationship(myNewNode, nodeAux, RelTypes.XREF);
        }
        // Reactant outgoing
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.REACTANT, Direction.OUTGOING)) {
            addRelationship(myNewNode, nodeAux, RelTypes.REACTANT);
        }
        // Reactant incoming
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.REACTANT, Direction.INCOMING)) {
            addRelationship(nodeAux, myNewNode, RelTypes.REACTANT);
        }
        // Controller
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.CONTROLLER, Direction.OUTGOING)) {
            addRelationship(myNewNode, nodeAux, RelTypes.CONTROLLER);
        }
        // Controlled
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.CONTROLLED, Direction.INCOMING)) {
            addRelationship(nodeAux, myNewNode, RelTypes.CONTROLLED);
        }
        // TODO: This will launch an exception if the nodes still contain relationships
        node1.delete();
        node2.delete();
*/
    }

    /**
     * Method necessary to merge nodes. This method will check for a pair of nodes, the nodes that can be achieved
     * given the same relationship and direction and return the set comprised by the two of them.
     * All the relationships from node1 and node2 to the set returned by the method will be removed from the database.
     *
     * @param node1 Node
     * @param node2 Node
     * @param relation Relationship to follow
     */
    private Set<Node> getUniqueNodes(Node node1, Node node2, RelTypes relation) {
        Set<Node> myUniqueNodes = new HashSet<>();
        // TODO: Be sure that this set only stores non-repeated nodes
//        for (Relationship relationShip : node1.getRelationships(relation, direction)) {
//            myUniqueNodes.add(relationShip.getOtherNode(node1));
//            relationShip.delete();
//        }
//        for (Relationship relationShip : node2.getRelationships(relation, direction)) {
//            myUniqueNodes.add(relationShip.getOtherNode(node2));
//            relationShip.delete();
//        }
        return myUniqueNodes;
    }

    /**
     * Parses the Node node into an ontology bean.
     *
     * @param node
     * @return
     * @throws BioNetDBException
     */
    private Ontology node2Ontology(Node node) throws BioNetDBException {
        Ontology myOntology = new Ontology();
//        if (node.hasProperty("source")) {
//            myOntology.setSource((String) node.getProperty("source"));
//        }
//        if (node.hasProperty("sourceVersion")) {
//            myOntology.setSource((String) node.getProperty("sourceVersion"));
//        }
//        if (node.hasProperty("id")) {
//            myOntology.setSource((String) node.getProperty("id"));
//        }
//        if (node.hasProperty("idVersion")) {
//            myOntology.setSource((String) node.getProperty("idVersion"));
//        }
//        if (node.hasProperty("name")) {
//            myOntology.setSource((String) node.getProperty("name"));
//        }
//        if (node.hasProperty("description")) {
//            myOntology.setSource((String) node.getProperty("description"));
//        }
        return myOntology;
    }


//    private CellularLocation node2CellularLocation(Node node) throws BioNetDBException {
//        CellularLocation myCellularLocation = new CellularLocation();
//        if (node.hasProperty("id")) {
//            myCellularLocation.setName((String) node.getProperty("id"));
//        }
//        if (node.hasRelationship(RelTypes.CEL_ONTOLOGY, Direction.OUTGOING)) {
//            List<Ontology> myOntologies = new ArrayList<>();
//            for (Relationship myRelationship : node.getRelationships(RelTypes.CEL_ONTOLOGY, Direction.OUTGOING)) {
//                myOntologies.add(node2Ontology(myRelationship.getEndNode()));
//            }
//            myCellularLocation.setOntologies(myOntologies);
//        }
//        return myCellularLocation;
//    }


//    private PhysicalEntity node2PhysicalEntity(Node node) throws BioNetDBException {
//        PhysicalEntity p = null;
//        switch ((PhysicalEntity.Type) node.getProperty("type")) {
//            case COMPLEX:
//                p = new Complex();
//                break;
//            case UNDEFINED:
//                p = new Undefined();
//                break;
//            case PROTEIN:
//                p = new Protein();
//                break;
//            case DNA:
//                p = new Dna();
//                break;
//            case RNA:
//                p = new Rna();
//                break;
//            case SMALL_MOLECULE:
//                p = new SmallMolecule();
//                break;
//            default:
//                break;
//        }
//        if (p == null) {
//            throw new BioNetDBException("The node intended to be parsed to a Physical Entity seems not to be a proper"
//                    + "Physical Entity node");
//        } else {
//            p.setId((String) node.getProperty("id"));
//            p.setName((String) node.getProperty("name"));
//            p.setDescription((List<String>) node.getProperty("description"));
//            p.setSource((List<String>) node.getProperty("source"));
//
//            if (node.hasRelationship(RelTypes.ONTOLOGY)) {
//                List<Ontology> ontologyList = new ArrayList<>();
//                for (Relationship relationship : node.getRelationships(Direction.OUTGOING, RelTypes.ONTOLOGY)) {
//                    Node ontologyNode = relationship.getEndNode();
//                    ontologyList.add(node2Ontology(ontologyNode));
//                }
//                p.setOntologies(ontologyList);
//            }
//
//            if (node.hasRelationship(RelTypes.CELLULARLOCATION)) {
//                List<CellularLocation> cellularLocationList = new ArrayList<>();
//                for (Relationship relationship : node.getRelationships(Direction.OUTGOING, RelTypes.CELLULARLOCATION)) {
//                    Node cellularLocationNode = relationship.getEndNode();
//                    cellularLocationList.add(node2CellularLocation(cellularLocationNode));
//                }
//                p.setCellularLocation(cellularLocationList);
//            }
//        }
//
//        return p;
//    }

    @Override
    public QueryResult get(Query query, QueryOptions queryOptions) throws BioNetDBException {
        long startTime = System.currentTimeMillis();
        String myQuery = Neo4JQueryParser.parse(query, queryOptions);
        long stopTime = System.currentTimeMillis();
        // TODO: Build new Network with the result
        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("get", time, 0, 0, null, null, Arrays.asList(new Network()));
    }

    @Override
    public QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions) {
        return null;
    }

    private int getTotalNodes() {
        return this.session.run("MATCH (n) RETURN count(n) AS count").peek().get("count").asInt();
    }

    private int getTotalRelationships() {
        return this.session.run("MATCH ()-[r]-() RETURN count(r) AS count").peek().get("count").asInt();
    }

    private ObjectMap getTotalPhysicalEntities() {
        ObjectMap myResult = new ObjectMap();
        myResult.put("undefined", this.session.run(("match (n:PhysicalEntity {type:\""
                + PhysicalEntity.Type.UNDEFINED + "\" }) return count(n) AS count")).peek().get("count").asInt());
        myResult.put("protein", this.session.run(("match (n:PhysicalEntity {type: \""
                + PhysicalEntity.Type.PROTEIN + "\"}) return count(n) AS count")).peek().get("count").asInt());
        myResult.put("dna", this.session.run(("match (n:PhysicalEntity {type: \""
                + PhysicalEntity.Type.DNA + "\"}) return count(n) AS count")).peek().get("count").asInt());
        myResult.put("rna", this.session.run(("match (n:PhysicalEntity {type: \""
                + PhysicalEntity.Type.RNA + "\"}) return count(n) AS count")).peek().get("count").asInt());
        myResult.put("complex", this.session.run(("match (n:PhysicalEntity {type: \""
                + PhysicalEntity.Type.COMPLEX + "\"}) return count(n) AS count")).peek().get("count").asInt());
        myResult.put("small_molecule", this.session.run(("match (n:PhysicalEntity {type: \""
                + PhysicalEntity.Type.SMALL_MOLECULE + "\"}) return count(n) AS count")).peek().get("count").asInt());
//       myResult.put("undefined", Integer.parseInt(this.database.execute("match (n:PhysicalEntity {type:\""
//                + PhysicalEntity.Type.UNDEFINED + "\" }) return count(n)")
//                .columnAs("count(n)").next().toString()));
//        myResult.put("protein", Integer.parseInt(this.database.execute("match (n:PhysicalEntity {type: \""
//                + PhysicalEntity.Type.PROTEIN + "\"}) return count(n)").columnAs("count(n)").next().toString()));
//        myResult.put("dna", Integer.parseInt(this.database.execute("match (n:PhysicalEntity {type: \""
//                + PhysicalEntity.Type.DNA + "\"}) return count(n)").columnAs("count(n)").next().toString()));
//        myResult.put("rna", Integer.parseInt(this.database.execute("match (n:PhysicalEntity {type: \""
//                + PhysicalEntity.Type.RNA + "\"}) return count(n)").columnAs("count(n)").next().toString()));
//        myResult.put("complex", Integer.parseInt(this.database.execute("match (n:PhysicalEntity {type: \""
//                + PhysicalEntity.Type.COMPLEX + "\"}) return count(n)").columnAs("count(n)").next().toString()));
//        myResult.put("small_molecule", Integer.parseInt(this.database.execute("match (n:PhysicalEntity {type: \""
//                + PhysicalEntity.Type.SMALL_MOLECULE + "\"}) return count(n)").columnAs("count(n)").next().toString()));
        int total = 0;
        for (String key : myResult.keySet()) {
            total += (int) myResult.get(key);
        }
        myResult.put("totalPE", total);
        return myResult;
    }

    private int getTotalXrefNodes() {
        return this.session.run("MATCH (n:Xref) RETURN count(n) AS count").peek().get("count").asInt();
//        return Integer.parseInt(this.database.execute("MATCH (n:Xref) RETURN count(n)")
//                .columnAs("count(n)").next().toString());
    }

    private int getTotalXrefRelationships() {
        return this.session.run("MATCH (n:PhysicalEntity)-[r:XREF]->(m:Xref) RETURN count(r) AS count").peek().get("count").asInt();
//        return Integer.parseInt(this.database.execute("MATCH (n:PhysicalEntity)-[r:XREF]->(m:Xref) RETURN count(r)")
//                .columnAs("count(r)").next().toString());
    }

    private int getTotalOntologyNodes() {
        return this.session.run("MATCH (n:Ontology) RETURN count(n) AS count").peek().get("count").asInt();
//        return Integer.parseInt(this.database.execute("MATCH (n:Ontology) RETURN count(n)")
//                .columnAs("count(n)").next().toString());
    }

    private int getTotalOntologyRelationships() {
        return this.session.run("MATCH (n)-[r:ONTOLOGY|CEL_ONTOLOGY]->(m:Ontology) RETURN count(r) AS count").peek().get("count").asInt();
//        return Integer.parseInt(this.database.execute(
//                "MATCH (n)-[r:ONTOLOGY|CEL_ONTOLOGY]->(m:Ontology) RETURN count(r)")
//                .columnAs("count(r)").next().toString());
    }

    private int getTotalCelLocationNodes() {
        return this.session.run("MATCH (n:CellularLocation) RETURN count(n) AS count").peek().get("count").asInt();
//        return Integer.parseInt(this.database.execute("MATCH (n:CellularLocation) RETURN count(n)")
//                .columnAs("count(n)").next().toString());
    }

    private int getTotalCelLocationRelationships() {
        return this.session.run("MATCH (n:PhysicalEntity)-[r:CELLULARLOCATION]->(m:CellularLocation) RETURN count(r) AS count")
                .peek().get("count").asInt();
//        return Integer.parseInt(this.database.execute(
//                "MATCH (n:PhysicalEntity)-[r:CELLULARLOCATION]->(m:CellularLocation) RETURN count(r)")
//                .columnAs("count(r)").next().toString());
    }

    @Override
    public QueryResult getSummaryStats(Query query, QueryOptions queryOptions) {
        long startTime = System.currentTimeMillis();
        ObjectMap myOutput = getTotalPhysicalEntities();
        myOutput.put("totalNodes", getTotalNodes());
        myOutput.put("totalRelations", getTotalRelationships());
        myOutput.put("totalXrefNodes", getTotalXrefNodes());
        myOutput.put("totalXrefRelations", getTotalXrefRelationships());
        myOutput.put("totalOntologyNodes", getTotalOntologyNodes());
        myOutput.put("totalOntologyRelations", getTotalOntologyRelationships());
        myOutput.put("totalCelLocationNodes", getTotalCelLocationNodes());
        myOutput.put("totalCelLocationRelations", getTotalCelLocationRelationships());
        int time = (int) (System.currentTimeMillis() - startTime);

        return new QueryResult<>("stats", time, 1, 1, null, null, Arrays.asList(myOutput));
    }

    @Override
    public QueryResult betweenness(Query query) {
        String id = query.getString("id");

        String nodeLabel = query.getString("nodeLabel", "PhysicalEntity");
        nodeLabel = nodeLabel.replace(",", "|");

        String relationshipLabel = query.getString("relationshipLabel", "REACTANT");
        relationshipLabel = relationshipLabel.replace(",", "|");

        StringBuilder cypherQuery = new StringBuilder();
        cypherQuery.append("MATCH p = allShortestPaths((source:"
                + nodeLabel + ")-[r:" + relationshipLabel + "*]->(destination:" + nodeLabel + "))");
        cypherQuery.append(" WHERE source <> destination AND LENGTH(NODES(p)) > 1");
        cypherQuery.append(" WITH EXTRACT(n IN NODES(p)| n.name) AS nodes");
        cypherQuery.append(" RETURN HEAD(nodes) AS source,");
        cypherQuery.append(" HEAD(TAIL(TAIL(nodes))) AS destination,");
        cypherQuery.append(" COLLECT(nodes) AS paths");
        /*
        Result execute = this.database.execute(cypherQuery.toString());
        while (execute.hasNext()) {
            Map<String, Object> next = execute.next();
            System.out.println(next.toString());
        }
        */

        return null;
    }

    @Override
    public QueryResult clusteringCoefficient(Query query) {
        // The clustering coefficient of a node is defined as the probability that two randomly
        // selected neighbors are connected to each other. With the number of neighbors as n and
        // the number of mutual connections between the neighbors r the calculation is:
        // clusteringCoefficient = r/NumberOfPossibleConnectionsBetweenTwoNeighbors. Where:
        // NumberOfPossibleConnectionsBetweenTwoNeighbors: n!/(2!(n-2)!).

        long startTime = System.currentTimeMillis();

        String ids = query.getString("id");
        ids = ids.replace(",", "\",\"");
        ids = ids.replace("|", "\",\"");

        StringBuilder cypherQuery = new StringBuilder();
        cypherQuery.append("UNWIND[\"" + ids + "\"] AS i");
        cypherQuery.append(" MATCH (x:Xref { id: i })--(a)--(:Interaction)--(b)");
        cypherQuery.append(" WITH a, count(DISTINCT b) AS n");
        cypherQuery.append(" MATCH (a)--(:Interaction)--(:PhysicalEntity)"
                + "--(:Interaction)-[r]-(:PhysicalEntity)--(:Interaction)--(a)");
        cypherQuery.append(" MATCH (a)-[:CELLULARLOCATION]-(c:CellularLocation)");
        cypherQuery.append(" RETURN a.name, c.id, n, count(DISTINCT r) AS r");

        System.out.println(cypherQuery.toString());
//        Result execute = this.database.execute(cypherQuery.toString());
        StatementResult execute = this.session.run(cypherQuery.toString());

        StringBuilder sb = new StringBuilder();
        if (execute.hasNext()) {
            sb.append("#ID\tLOCATION\tCLUSTERING_COEFFICIENT\n");
            while (execute.hasNext()) {
//                Map<String, Object> result = execute.next();
                Record result = execute.next();
                Integer r = (int) result.get("r").asLong();
                Integer n = (int) result.get("n").asLong();

                sb.append("\"" + result.get("a.name").toString() + "\"\t"
                        + "\"" + result.get("c.id").toString() + "\"\t");

                // Computed value must fit into a double. The largest n for which n! < Double.MAX_VALUE is 170.
                if (n > 170) {
                    sb.append("\"NA\"\n");
                } else if (n > 1) {
                    double possibleConnexions = CombinatoricsUtils.factorialDouble(n)
                            / (CombinatoricsUtils.factorialDouble(2) * (CombinatoricsUtils.factorialDouble(n - 2)));
                    DecimalFormat df = new DecimalFormat("###.##");
                    sb.append("\"" + df.format(r / possibleConnexions) + "\"\n");
                } else {
                    sb.append("\"0.00\"\n");
                }
            }
        }

        int time = (int) (System.currentTimeMillis() - startTime);

        return new QueryResult<>("clustCoeff", time, 1, 1, null, null, Arrays.asList(sb.toString()));
    }

    public boolean isClosed() {
//        return !this.openedDB;
        return !this.session.isOpen();
    }

    public void close() {
//        this.database.shutdown();
        this.session.close();
        this.driver.close();
    }

}
