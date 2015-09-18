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
                //            .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
                //            .setConfig(GraphDatabaseSettings.relationship_auto_indexing, "true")
                //            .setConfig(GraphDatabaseSettings.node_keys_indexable, "id")
                .newGraphDatabase();

        try (Transaction tx = this.database.beginTx()) {
            Schema schema = this.database.schema();
            schema.indexFor( DynamicLabel.label( "PhysicalEntity" ) )
                    .on( "id" )
                    .create();
            schema.indexFor(DynamicLabel.label("PhysicalEntity"))
                    .on( "name" )
                    .create();

            schema.indexFor( DynamicLabel.label( "Xref" ) )
                    .on( "id" )
                    .create();

            schema.indexFor(DynamicLabel.label("Tissue"))
                    .on( "tissue" )
                    .create();

            schema.indexFor(DynamicLabel.label("TimeSeries"))
                    .on( "timeseries" )
                    .create();

            schema.indexFor( DynamicLabel.label( "Interaction" ) )
                    .on( "id" )
                    .create();
            schema.indexFor(DynamicLabel.label("Interaction"))
                    .on( "name" )
                    .create();

            tx.success();
        }

    }

    private enum RelTypes implements RelationshipType {
        REACTANT,
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
     * Method to annotate Xrefs in the database
     * @param nodeID ID of the node we want to annotate
     * @param xref_list List containing all the Xref annotations to be added in the database
     */
    @Override
    public void addXrefs(String nodeID, List<Xref> xref_list) {
        try ( Transaction tx = this.database.beginTx()) {
            Node xrefNode = getNode("Xref", new ObjectMap("id", nodeID));
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

    private ObjectMap parsePhysicalEntity (PhysicalEntity myPhysicalEntity) {
        ObjectMap myOutput = new ObjectMap("id", myPhysicalEntity.getId());
        if (myPhysicalEntity.getName() != null) {
            myOutput.put("name", myPhysicalEntity.getName());
        }
        if (myPhysicalEntity.getDescription() != null) {
            myOutput.put("description",myPhysicalEntity.getDescription());
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
     * Insert all the elements present in the Ontology object into an ObjectMap object
     * @param myOntology
     * @return ObjectMap object containing the values present in myOntology
     */
    private ObjectMap parseOntology (Ontology myOntology) {
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
     * Insert all the elements present in the CellularLocation object into an ObjectMap object
     * @param myCellularLocation
     * @return ObjectMap object containing the values present in myCellularLocation
     */
    private ObjectMap parseCellularLocation (CellularLocation myCellularLocation) {
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

    /**
     * Insert physical entities into the Neo4J database
     * @param physicalEntityList List containing all the physical entities to be inserted in the database
     * @param queryOptions
     */
    private void insertPhysicalEntities(List<PhysicalEntity> physicalEntityList, QueryOptions queryOptions) throws NetworkDBException {
        try ( Transaction tx = this.database.beginTx() ) {
            // 1. Insert the Physical Entities and the basic nodes they are connected to
            for (PhysicalEntity p : physicalEntityList) {
                // StringBuilder xrefs = new StringBuilder();
                // 1. Check if the Xrefs are already inserted in the database within other node
                List<Node> nodesToMerge = new ArrayList<>();
                for (Xref xref : p.getXrefs()) {
                    Node aux_xref = getNode("Xref", new ObjectMap("id", xref.getId()));
                    if (aux_xref != null) {
                        // xrefs.append(xref.getId() + " - ");
                        // Add the node that the current PE has to be merged with
                        nodesToMerge.add(aux_xref.getSingleRelationship(RelTypes.XREF, Direction.INCOMING).getStartNode());
                    }
                }
                // 2. Insert or merge the current PE
                //              if (nodesToMerge.size() == 0) {
                // Insert all the Physical entity nodes
                Node n = getOrCreateNode("PhysicalEntity", parsePhysicalEntity(p));
                //Node n = getOrCreateNode("PhysicalEntity", myProperties);
                //addXrefNode(n, new Xref(null, null, p.getId(), null));

                // 2.1. Insert the ontologies
                for (Ontology o : p.getOntologies()) {
                    Node ont = getOrCreateNode("Ontology", parseOntology(o));
                    addInteraction(n, ont, RelTypes.ONTOLOGY);
                }

                    /* Insert the cellular locations */
                for (CellularLocation c : p.getCellularLocation()) {
                    Node cel = getOrCreateCellularLocationNode(parseCellularLocation(c));
                    addInteraction(n, cel, RelTypes.CELLULARLOCATION);
                }

                // 2.1. Insert the Xrefs
                for (Xref xref : p.getXrefs()) {
                    addXrefNode(n, xref);
                }
    /*
                    } else {
                        // System.out.println(nodesToMerge.get(0).getProperty("id") + " = " + p.getId() + ": " + xrefs.toString());
                        // TODO: merge the nodes... Remember that we could be trying to reinsert the same network

                    }
    */
                // 3. Insert or merge Xrefs...

            }
            // 2. Insert the existing relationships between Physical Entities
            for (PhysicalEntity p : physicalEntityList) {
                if (p.getComponentOfComplex().size() > 0) {
                    Node pe_node = getNode("PhysicalEntity", new ObjectMap("id", p.getId()));
                    if (pe_node == null) {
                        throw new NetworkDBException("Physical entities are not properly inserted in the database. " +
                                "Cannot find a physical entity that is supposed to exist.");
                    }
                    for (String complexID : p.getComponentOfComplex()) {
                        Node complex_node = getNode("PhysicalEntity", new ObjectMap("id", complexID));
                        if (complex_node == null) {
                            throw new NetworkDBException("Complex: Physical entities are not properly inserted in " +
                                    "the database. Cannot find a physical entity that is supposed to exist.");
                        }
                        if (complex_node.getProperty("type").equals(PhysicalEntity.Type.COMPLEX.toString())) {
                            addInteraction(pe_node, complex_node, RelTypes.COMPONENTOFCOMPLEX);
                        } else {
                            throw new NetworkDBException("The relationship 'Component of complex' cannot be created " +
                                    "because the destiny node is not of type complex. Check Physical Entity " +
                                    complexID);
                        }
                    }
                }
            }
            tx.success();
        }

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
                    mynode.setProperty("description", i.getDescription().toArray(new String[0]));
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

    private Node getNode (String label, ObjectMap properties) {
        IndexManager index = database.index();
        //        System.out.println("Arrays.toString(index.nodeIndexNames()) = " + Arrays.toString(index.nodeIndexNames()));
        // TODO: Considering all the properties as String. This has to be changed.
        // TODO: At the moment, all the properties I'm inserting are strings. However, when issue #18 gets resolved, we should change the insertion of properties.
        // Cypher query
        Node n = this.database.findNode(DynamicLabel.label(label), "id", properties.get("id"));
            /*
            StringBuilder myquery = new StringBuilder();
            myquery.append("MATCH (n:").append(label).append(") WHERE ");
    /*        for (String key : properties.keySet()) {
                myquery.append("n.")
                        .append(key)
                        .append("= \"")
                        .append(properties.getString(key))
                        .append("\" AND ");
            }
            myquery.setLength(myquery.length() - 4); // Remove last AND
            myquery.append("RETURN n");
            */
            /*
            myquery.append("n.id = \"")
                    .append(properties.get("id"))
                    .append("\" RETURN n");


            Result result = this.database.execute(myquery.toString());
            if (result.hasNext()) {
                return (Node) result.next().get("n");
            } else {
                return null;
            }
            */
        return n;
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
        }
        return mynode;
    }

    /**
     * Checks if the cellular location given in properties is already in database. If not, it creates it and
     * returns the node.
     * @param properties
     * @return
     */
    private Node getOrCreateCellularLocationNode (ObjectMap properties) {
        Node mynode = getOrCreateNode("CellularLocation", new ObjectMap("id", properties.get("id")));
        // gets or creates ontology node
        if (properties.containsKey("ontologies")) {
            for (ObjectMap myOntology : (List<ObjectMap>) properties.get("ontologies")) {
                Node ontNode = getOrCreateNode("Ontology", myOntology);
                addInteraction(mynode, ontNode, RelTypes.CEL_ONTOLOGY);
            }
        }
        return mynode;
    }

    // TODO: Rename to addRelationship
    /**
     * The function will create an interaction between two nodes if the relation does not exist
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

        Node xref_node = getOrCreateNode("Xref", myProperties);

        if (!xref_node.hasRelationship(RelTypes.XREF, Direction.INCOMING))
            node.createRelationshipTo(xref_node, RelTypes.XREF);
    }

    /**
     * This method will be called every time we consider that two existing nodes are the same and should be merged
     * @param node1
     * @param node2
     */
    // TODO: Maybe we would have to add the type of nodes we want to merge... Now it is done considering they are PE
    private void mergeNodes (Node node1, Node node2) {
        Node myNewNode = null;
        // TODO: 1. Merge the basic information from both nodes into this one.
        // 2. Destroy the relationships present in both nodes and apply them to the new node.
        // TODO: Check expression data
        // TODO: What would happen if two different nodes have different expression data....??
        // TODO: What if both of them have expression for the same tissues, but for different timeseries??
        // Expression data
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.TISSUE, Direction.OUTGOING)) {
            addInteraction(myNewNode, nodeAux, RelTypes.TISSUE);
        }
        // Ontologies
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.ONTOLOGY, Direction.OUTGOING)) {
            addInteraction(myNewNode, nodeAux, RelTypes.ONTOLOGY);
        }
        // Xrefs
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.XREF, Direction.OUTGOING)) {
            addInteraction(myNewNode, nodeAux, RelTypes.XREF);
        }
        // Reactant outgoing
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.REACTANT, Direction.OUTGOING)) {
            addInteraction(myNewNode, nodeAux, RelTypes.REACTANT);
        }
        // Reactant incoming
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.REACTANT, Direction.INCOMING)) {
            addInteraction(nodeAux, myNewNode, RelTypes.REACTANT);
        }
        // Controller
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.CONTROLLER, Direction.OUTGOING)) {
            addInteraction(myNewNode, nodeAux, RelTypes.CONTROLLER);
        }
        // Controlled
        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.CONTROLLED, Direction.INCOMING)) {
            addInteraction(nodeAux, myNewNode, RelTypes.CONTROLLED);
        }
        // TODO: This will launch an exception if the nodes still contain relationships
        node1.delete();
        node2.delete();
    }

    /**
     * Method necessary to merge nodes. This method will check for a pair of nodes, the nodes that can be achieved
     * given the same relationship and direction and return the set comprised by the two of them.
     * All the relationships from node1 and node2 to the set returned by the method will be removed from the database.
     * @param node1 Node
     * @param node2 Node
     * @param relation Relationship to follow
     * @param direction Direction of the relationship
     */
    private Set<Node> getUniqueNodes (Node node1, Node node2, RelTypes relation, Direction direction) {
        Set<Node> myUniqueNodes = new HashSet<>();
        // TODO: Be sure that this set only stores non-repeated nodes
        for (Relationship relationShip : node1.getRelationships(relation, direction)) {
            myUniqueNodes.add(relationShip.getOtherNode(node1));
            relationShip.delete();
        }
        for (Relationship relationShip : node2.getRelationships(relation, direction)) {
            myUniqueNodes.add(relationShip.getOtherNode(node2));
            relationShip.delete();
        }
        return myUniqueNodes;
    }

    /**
     * Parses the Node node into an ontology bean
     * @param node
     * @return
     * @throws NetworkDBException
     */
    private Ontology Node2Ontology (Node node) throws NetworkDBException {
        Ontology myOntology = new Ontology();
        if (node.hasProperty("source")) {
            myOntology.setSource((String) node.getProperty("source"));
        }
        if (node.hasProperty("sourceVersion")) {
            myOntology.setSource((String) node.getProperty("sourceVersion"));
        }
        if (node.hasProperty("id")) {
            myOntology.setSource((String) node.getProperty("id"));
        }
        if (node.hasProperty("idVersion")) {
            myOntology.setSource((String) node.getProperty("idVersion"));
        }
        if (node.hasProperty("name")) {
            myOntology.setSource((String) node.getProperty("name"));
        }
        if (node.hasProperty("description")) {
            myOntology.setSource((String) node.getProperty("description"));
        }
        return myOntology;
    }

    /**
     * Returns Parses the Node node into a cellular location bean
     * @param node
     * @return
     * @throws NetworkDBException
     */
    private CellularLocation Node2CellularLocation (Node node) throws NetworkDBException {
        CellularLocation myCellularLocation = new CellularLocation();
        if (node.hasProperty("id")) {
            myCellularLocation.setName((String) node.getProperty("id"));
        }
        if (node.hasRelationship(RelTypes.CEL_ONTOLOGY, Direction.OUTGOING)) {
            List<Ontology> myOntologies = new ArrayList<>();
            for (Relationship myRelationship : node.getRelationships(RelTypes.CEL_ONTOLOGY, Direction.OUTGOING)) {
                myOntologies.add(Node2Ontology(myRelationship.getEndNode()));
            }
            myCellularLocation.setOntologies(myOntologies);
        }
        return myCellularLocation;
    }

    /**
     * This method will parse the node information into a Physical Entity object
     * @param node
     * @return PhysicalEntity object
     */
    private PhysicalEntity Node2PhysicalEntity (Node node) throws NetworkDBException {
        PhysicalEntity p = null;
        switch ((PhysicalEntity.Type) node.getProperty("type")) {
            case COMPLEX:
                p = new Complex();
                break;
            case UNDEFINEDENTITY:
                p = new UndefinedEntity();
                break;
            case PROTEIN:
                p = new Protein();
                break;
            case DNA:
                p = new Dna();
                break;
            case RNA:
                p = new Rna();
                break;
            case SMALLMOLECULE:
                p = new SmallMolecule();
                break;
            default:
                break;
        }
        if (p == null) {
            throw new NetworkDBException("The node intended to be parsed to a Physical Entity seems not to be a proper" +
                    "Physical Entity node");
        } else {
            p.setId((String) node.getProperty("id"));
            p.setName((String) node.getProperty("name"));
            p.setDescription((List<String>) node.getProperty("description"));
            p.setSource((List<String>) node.getProperty("source"));

            if (node.hasRelationship(RelTypes.ONTOLOGY)) {
                List<Ontology> ontologyList = new ArrayList<>();
                for (Relationship relationship : node.getRelationships(Direction.OUTGOING, RelTypes.ONTOLOGY)) {
                    Node ontologyNode = relationship.getEndNode();
                    ontologyList.add(Node2Ontology(ontologyNode));
                }
                p.setOntologies(ontologyList);
            }

            if (node.hasRelationship(RelTypes.CELLULARLOCATION)) {
                List<CellularLocation> cellularLocationList = new ArrayList<>();
                for (Relationship relationship : node.getRelationships(Direction.OUTGOING, RelTypes.CELLULARLOCATION)) {
                    Node cellularLocationNode = relationship.getEndNode();
                    cellularLocationList.add(Node2CellularLocation(cellularLocationNode));
                }
                p.setCellularLocation(cellularLocationList);
            }
        }

        return p;
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
