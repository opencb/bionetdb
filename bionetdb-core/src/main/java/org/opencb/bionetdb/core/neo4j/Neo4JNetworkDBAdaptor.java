package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.PhysicalEntity;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private Driver driver;

    private BioNetDBConfiguration configuration;
    private CellBaseClient cellBaseClient;

    private final String PREFIX_ATTRIBUTES = "attr_";

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration) throws BioNetDBException {
        this(database, configuration, false);
    }

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration, boolean createIndex) throws BioNetDBException {
        this.configuration = configuration;

        DatabaseConfiguration databaseConfiguration = getDatabaseConfiguration(database);
        if (databaseConfiguration == null) {
            throw new BioNetDBException("No database found with name: \"" + database + "\"");
        }
        String databaseURI = databaseConfiguration.getHost() + ":" + databaseConfiguration.getPort();
        String user = databaseConfiguration.getUser();
        String password = databaseConfiguration.getPassword();

        driver = GraphDatabase.driver("bolt://" + databaseURI, AuthTokens.basic(user, password));
//        session = driver.session();

//        registerShutdownHook(this.driver, this.session);
        registerShutdownHook(this.driver);

        if (createIndex) {
            createIndexes();
        }
    }

    private void registerShutdownHook(final Driver driver) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
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
        Session session = this.driver.session();
        if (session != null) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :" + Node.Type.PHYSICAL_ENTITY + "(uid)");
                //             tx.run("CREATE INDEX ON :" + NodeTypes.PHYSICAL_ENTITY + "(name)");

                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.COMPLEX + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.SMALL_MOLECULE + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.CATALYSIS + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.REACTION + "(uid)");
                //               tx.run("CREATE INDEX ON :" + NodeTypes.INTERACTION + "(name)");

                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(uid)");
//                tx.run("CREATE INDEX ON :" + NodeTypes.XREF + "(source)");
//
                tx.run("CREATE INDEX ON :" + Node.Type.ONTOLOGY + "(uid)");
//                tx.run("CREATE INDEX ON :" + NodeTypes.ONTOLOGY + "(source)");
//                tx.run("CREATE INDEX ON :" + NodeTypes.ONTOLOGY + "(name)");

//                tx.run("CREATE INDEX ON :Tissue(tissue)");
//                tx.run("CREATE INDEX ON :TimeSeries(timeseries)");

                tx.run("CREATE INDEX ON :" + PhysicalEntity.Type.UNDEFINED + "(uid)");

                tx.success();
            }
            session.close();
        }
    }



    /**
     * Insert an entire network into the Neo4J database.
     *
     * @param network      Object containing all the nodes and interactions
     * @param queryOptions Optional params
     */
    @Override
    public void insert(Network network, QueryOptions queryOptions) throws BioNetDBException {

        Session session = this.driver.session();

        // First, insert Neo4J nodes
        for (Node node: network.getNodes()) {
            session.writeTransaction(tx -> {
                addNode(tx, node);
                return 1;
            });
        }

        // Second, insert Neo4J relationships
        for (Relation relation: network.getRelations()) {
            session.writeTransaction(tx -> {
                addRelation(tx, relation);
                return 1;
            });
        }

        session.close();
    }

    private StatementResult addNode(Transaction tx, Node node) {
        // Gather properties of the node to create a cypher string with them
        List<String> props = new ArrayList<>();
        props.add("uid:" + node.getUid());
        if (StringUtils.isNotEmpty(node.getId())) {
            props.add("id:\"" + cleanValue(node.getId()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getName())) {
            props.add("name:\"" + cleanValue(node.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getSource())) {
            props.add("source:\"" + node.getSource() + "\"");
        }
        for (String key : node.getAttributes().keySet()) {
            if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                props.add(PREFIX_ATTRIBUTES + key + ":" + node.getAttributes().getString(key));
            } else {
                props.add(PREFIX_ATTRIBUTES + key + ":\"" + cleanValue(node.getAttributes().getString(key)) + "\"");
            }
        }
        String propsJoined = "{" + String.join(",", props) + "}";

        // Create the desired node
        return tx.run("CREATE (n:" + StringUtils.join(node.getTags(), ":") + " " + propsJoined + ") RETURN ID(n) AS ID");
    }

    private StatementResult addRelation(Transaction tx, Relation relation) {
        List<String> props = new ArrayList<>();
        props.add("uid:" + relation.getUid());
        if (StringUtils.isNotEmpty(relation.getName())) {
            props.add("name:\"" + cleanValue(relation.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(relation.getSource())) {
            props.add("source:\"" + relation.getSource() + "\"");
        }
        for (String key : relation.getAttributes().keySet()) {
            props.add(PREFIX_ATTRIBUTES + key + ":\"" + cleanValue(relation.getAttributes().getString(key)) + "\"");
        }
        String propsJoined = "{" + String.join(",", props) + "}";

        StringBuilder statementTemplate = new StringBuilder();
        statementTemplate.append("MATCH (o) WHERE o.uid = $origUid")
                .append(" MATCH (d) WHERE d.uid = $destUiD")
                .append(" MERGE (o)-[:").append(StringUtils.join(relation.getTags(), ":")).append(propsJoined).append("]->(d)");

        // Create the relationship
        return tx.run(statementTemplate.toString(), parameters("origUid", relation.getOrigUid(), "destUiD", relation.getDestUid()));
    }

    private String cleanValue(String value) {
        return value.replace("\"", ",").replace("\\", "|");
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }


//
//    /**
//     * Method to annotate Xrefs in the database.
//     *
//     * @param nodeID   ID of the node we want to annotate
//     * @param xrefList List containing all the Xref annotations to be added in the database
//     */
//    @Override
//    public void addXrefs(String nodeID, List<Xref> xrefList) throws BioNetDBException {
//        Session session = this.driver.session();
//        session.writeTransaction(new TransactionWork<Integer>() {
//            @Override
//            public Integer execute(Transaction tx) {
//                StatementResult xrefNode = getNode(tx, NodeTypes.XREF.toString(), new ObjectMap("id", nodeID));
//                if (xrefNode.hasNext()) {
//                    // Look for the physical entity to which the xref is associated with
//                    StatementResult pE = tx.run("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ")-[" + RelTypes.XREF
//                            + "]->(x:" + NodeTypes.XREF + ") WHERE ID(x) = "
//                            + xrefNode.peek().get("ID").toString()
//                            + " RETURN ID(n) AS ID, LABELS(n) AS LABELS");
//                    for (Xref x : xrefList) {
//                        StatementResult myxref = getOrCreateNode(tx, NodeTypes.XREF.toString(), parseXref(x));
//                        addRelationship(tx, concatenateLabels(pE.peek().get("LABELS")), NodeTypes.XREF.toString(),
//                                pE.peek().get("ID").asInt(), myxref.peek().get("ID").asInt(), RelTypes.XREF);
//                    }
//                } else {
//                    return 0;
//                    //throw new BioNetDBException("The node to be annotated does not exist in the database.");
//                }
//                return 1;
//            }
//        });
//        session.close();
//    }
//
//    public void addVariants(List<Variant> variants) {
//        List<Gene> genes;
//        //List<Protein> proteins = getProteins(variant);
//
//        Session session = this.driver.session();
//        try (Transaction tx = session.beginTransaction()) {
//            for (Variant variant: variants) {
//                // Create the variant node
//                StatementResult variantNode = getOrCreateNode(tx, PhysicalEntity.Type.VARIANT.toString(), parseVariant(variant));
//
//                // Create study nodes
//                if (variant.getStudies() != null && ListUtils.isNotEmpty(variant.getStudies())) {
//                    for (StudyEntry studyEntry: variant.getStudies()) {
//                        StatementResult studyNode = getOrCreateNode(tx, NodeTypes.STUDY.toString(), parseStudyEntry(studyEntry));
//                        addRelationship(tx, PhysicalEntity.Type.VARIANT.toString(), NodeTypes.STUDY.toString(),
//                                variantNode.peek().get("ID").asInt(), studyNode.peek().get("ID").asInt(),
//                                RelTypes.STUDY);
//
//                        if (studyEntry.getSamplesData() != null && ListUtils.isNotEmpty(studyEntry.getSamplesData())) {
//                            int numSamples = studyEntry.getSamplesData().size();
//                            for (int i = 0; i < numSamples; i++) {
//                                // TODO: get sample name
//                                StatementResult sampleNode = getOrCreateNode(tx, NodeTypes.SAMPLE.toString(),
//                                        new ObjectMap("id", "Sample#0" + i));
//                                addRelationship(tx, NodeTypes.STUDY.toString(), NodeTypes.SAMPLE.toString(),
//                                        studyNode.peek().get("ID").asInt(), sampleNode.peek().get("ID").asInt(),
//                                        RelTypes.SAMPLE);
//                                StatementResult gtNode = createNode(tx, NodeTypes.GENOTYPE.toString(),
//                                        new ObjectMap("id", studyEntry.getSampleData(i).get(0)));
//                                addRelationship(tx, NodeTypes.SAMPLE.toString(), NodeTypes.GENOTYPE.toString(),
//                                        sampleNode.peek().get("ID").asInt(), gtNode.peek().get("ID").asInt(),
//                                        RelTypes.GENOTYPE);
//                                addRelationship(tx, PhysicalEntity.Type.VARIANT.toString(), NodeTypes.GENOTYPE.toString(),
//                                        variantNode.peek().get("ID").asInt(), gtNode.peek().get("ID").asInt(),
//                                        RelTypes.GENOTYPE);
//                            }
//                        }
//
//                        if (studyEntry.getFiles() != null && ListUtils.isNotEmpty(studyEntry.getFiles())) {
//                            for (FileEntry fileEntry: studyEntry.getFiles()) {
//                                StatementResult fileNode = getOrCreateNode(tx, NodeTypes.FILE.toString(), parseFileEntry(fileEntry));
//                                addRelationship(tx, NodeTypes.STUDY.toString(), NodeTypes.FILE.toString(),
//                                        studyNode.peek().get("ID").asInt(), fileNode.peek().get("ID").asInt(),
//                                        RelTypes.FILE);
//                                addRelationship(tx, PhysicalEntity.Type.VARIANT.toString(), NodeTypes.FILE.toString(),
//                                        variantNode.peek().get("ID").asInt(), fileNode.peek().get("ID").asInt(),
//                                        RelTypes.FILE);
//                            }
//                        }
//                    }
//                }
//
//                // Create population frequency nodes
//                if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
//                    for (PopulationFrequency popFreq: variant.getAnnotation().getPopulationFrequencies()) {
//                        StatementResult popFreqNode = getOrCreateNode(tx, NodeTypes.POPULATION_FREQUENCY.toString(),
//                                parsePopulationFrequency(popFreq));
//                        addRelationship(tx, PhysicalEntity.Type.VARIANT.toString(), NodeTypes.POPULATION_FREQUENCY.toString(),
//                                variantNode.peek().get("ID").asInt(), popFreqNode.peek().get("ID").asInt(),
//                                RelTypes.POPULATION_FREQUENCY);
//                    }
//                }
//
//                // Create consequence type nodes
//                if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
//                    for (ConsequenceType cs: variant.getAnnotation().getConsequenceTypes()) {
//                        StatementResult csNode = getOrCreateConsequenceTypeNode(tx, parseConsequenceType(cs));
//                        addRelationship(tx, PhysicalEntity.Type.VARIANT.toString(), NodeTypes.CONSEQUENCE_TYPE.toString(),
//                                variantNode.peek().get("ID").asInt(), csNode.peek().get("ID").asInt(),
//                                RelTypes.CONSEQUENCE_TYPE);
//                    }
//                }
//            }
//            tx.success();
//        }
//        session.close();
//    }
//
//    @Override
//    public void addExpressionData(String tissue, String timeSeries, List<Expression> myExpression, QueryOptions options) {
// /*       try (Transaction tx = this.database.beginTx()) {
//            for (Expression exprElem : myExpression) {
//                ObjectMap myProperty = new ObjectMap("id", exprElem.getId());
//                Node xrefNode = getNode("Xref", myProperty);
//                // parsing options
//                boolean addNodes = options.getBoolean("addNodes", false);
//
//                // If the node does not already exist, it does not make sense inserting expression data.
//                if (xrefNode == null && addNodes) {
//                    // Create basic node with info
//                    Node origin = createNode(NodeTypes.PHYSICAL_ENTITY., myProperty);
//                    addXrefNode(origin, new Xref(null, null, exprElem.getId(), null));
//                    xrefNode = getNode("Xref", myProperty);
//                }
//
//                if (xrefNode != null) {
//                    Node origin;
//                    for (Relation relationship : xrefNode.getRelations(RelTypes.XREF, Direction.INCOMING)) {
//                        origin = relationship.getStartNode();
//
//                        // Find or create tissueNode and the relationship
//                        Node tissueNode = null;
//                        for (Relation relationShip : origin.getRelations(RelTypes.TISSUE, Direction.OUTGOING)) {
//                            if (relationShip.getEndNode().getProperty("tissue").equals(tissue)) {
//                                tissueNode = relationShip.getEndNode();
//                                break;
//                            }
//                        }
//                        if (tissueNode == null) {
//                            myProperty = new ObjectMap();
//                            myProperty.put("tissue", tissue);
//                            tissueNode = createNode("Tissue", myProperty);
//                            addRelationship(origin, tissueNode, RelTypes.TISSUE);
//                        }
//
//                        // Find or create timeSeriesNode and the relationship
//                        Node timeSeriesNode = null;
//                        for (Relation relationShip : tissueNode.getRelations(RelTypes.TIMESERIES, Direction.OUTGOING)) {
//                            if (relationShip.getEndNode().getProperty("timeseries").equals(timeSeries)) {
//                                timeSeriesNode = relationShip.getEndNode();
//                                break;
//                            }
//                        }
//                        if (timeSeriesNode == null) {
//                            myProperty = new ObjectMap();
//                            myProperty.put("timeseries", timeSeries);
//                            timeSeriesNode = createNode("TimeSeries", myProperty);
//                            addRelationship(tissueNode, timeSeriesNode, RelTypes.TIMESERIES);
//                        }
//
//                        // Add or change the properties of the timeseries node in the database
//                        if (exprElem.getExpression() != -1) {
//                            timeSeriesNode.setProperty("expression", exprElem.getExpression());
//                        }
//                        if (exprElem.getPvalue() != -1) {
//                            timeSeriesNode.setProperty("pvalue", exprElem.getPvalue());
//                        }
//                        if (exprElem.getOdds() != -1) {
//                            timeSeriesNode.setProperty("odds", exprElem.getOdds());
//                        }
//                        if (exprElem.getUpregulated() != -1) {
//                            timeSeriesNode.setProperty("upregulated", exprElem.getUpregulated());
//                        }
//                    }
//                }
//            }
//            tx.success();
//        }
//*/
//    }
//
//
//    private ObjectMap parsePhysicalEntity(PhysicalEntity myPhysicalEntity) {
//        ObjectMap myOutput = new ObjectMap("id", myPhysicalEntity.getId());
//        if (StringUtils.isNotBlank(myPhysicalEntity.getName())) {
//            myOutput.put("name", myPhysicalEntity.getName());
//        }
///*
//        if (myPhysicalEntity.getDescription() != null) {
//            myOutput.put("description", myPhysicalEntity.getDescription());
//        }
//        */
//        if (myPhysicalEntity.getType() != null) {
//            myOutput.put("type", myPhysicalEntity.getType().name());
//        }
//        /*
//        if (myPhysicalEntity.getSource().size() > 0) {
//            myOutput.put("source", myPhysicalEntity.getSource());
//        }
//        */
//        return myOutput;
//    }
//
//    /***
//     * Insert all the elements present in the Ontology object into an ObjectMap object.
//     * @param myOntology Ontology object
//     * @return ObjectMap object containing the values present in myOntology
//     */
//    private ObjectMap parseOntology(Ontology myOntology) {
//        ObjectMap myOutput = new ObjectMap("id", myOntology.getId());
//        if (myOntology.getSource() != null) {
//            myOutput.put("source", myOntology.getSource());
//        }
//        if (myOntology.getDescription() != null) {
//            myOutput.put("description", myOntology.getDescription());
//        }
//        if (myOntology.getName() != null) {
//            myOutput.put("name", myOntology.getName());
//        }
//        if (myOntology.getIdVersion() != null) {
//            myOutput.put("idVersion", myOntology.getIdVersion());
//        }
//        if (myOntology.getSourceVersion() != null) {
//            myOutput.put("sourceVersion", myOntology.getSourceVersion());
//        }
//        return myOutput;
//    }
//
//    /***
//     * Insert all the elements present in the CellularLocation object into an ObjectMap object.
//     *
//     * @param myCellularLocation Cellular Location Object
//     * @return ObjectMap object containing the values present in myCellularLocation
//     */
//    private ObjectMap parseCellularLocation(CellularLocation myCellularLocation) {
//        ObjectMap myOutput = new ObjectMap("name", myCellularLocation.getName());
//        List<Ontology> myOntologies = myCellularLocation.getOntologies();
//        List<ObjectMap> allOntologies = new ArrayList<>();
//        if (myOntologies.size() > 0) {
//            for (Ontology myOntology : myOntologies) {
//                allOntologies.add(parseOntology(myOntology));
//            }
//        }
//        myOutput.put("ontologies", allOntologies);
//        return myOutput;
//    }
//
//    /***
//     * Insert all the elements present in the CellularLocation object into an ObjectMap object.
//     *
//     * @param xref Xref Object
//     * @return ObjectMap object containing the values present in myCellularLocation
//     */
//    private ObjectMap parseXref(Xref xref) {
//        ObjectMap myProperties = new ObjectMap();
//        if (xref.getSource() != null) {
//            myProperties.put("source", xref.getSource());
//        }
//        if (xref.getId() != null) {
//            myProperties.put("id", xref.getId());
//        }
//        if (xref.getSourceVersion() != null) {
//            myProperties.put("sourceVersion", xref.getSourceVersion());
//        }
//        if (xref.getIdVersion() != null) {
//            myProperties.put("idVersion", xref.getIdVersion());
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parseInteraction(Interaction interaction) {
//        ObjectMap myProperties = new ObjectMap();
//        if (interaction.getName() != null) {
//            myProperties.put("name", interaction.getName());
//        }
//        if (interaction.getId() != null) {
//            myProperties.put("id", interaction.getId());
//        }
//        if (interaction.getDescription() != null) {
//            myProperties.put("description", interaction.getDescription());
//        }
//        return myProperties;
//    }
//
//    private String concatenateLabels(Value labels) {
//        return labels.toString().replace("\", \"", ":").replaceAll("[\"\\[\\]]", "");
//    }
//
//    /**
//     * Insert nodes into the Neo4J database.
//     *
//     * @param nodeList      List containing all the nodes to be inserted in the database
//     * @param queryOptions  Additional params for the query
//     */
//    private void insertNodes(List<Node> nodeList, QueryOptions queryOptions) throws BioNetDBException {
//        Session session = this.driver.session();
//
//        // 1. Insert the Physical Entities and the basic nodes they are connected to
//        for (Node node: nodeList) {
//            session.writeTransaction(tx -> {
//                if (Node.isPhysicalEntity(node)
//                        && node.getType() != Node.Type.GENE) {
//                    PhysicalEntity p = (PhysicalEntity) node;
//                    String peLabel = NodeTypes.PHYSICAL_ENTITY + ":" + p.getType();
//                    StatementResult n = getOrCreateNode(tx, peLabel, parsePhysicalEntity(p));
//                    int pEID = n.peek().get("ID").asInt();
//
//                    // 1.1. Insert the ontologies
//                    for (Ontology o: p.getOntologies()) {
//                        StatementResult ont = getOrCreateNode(tx, NodeTypes.ONTOLOGY.toString(), parseOntology(o));
//                        addRelationship(tx, peLabel, NodeTypes.ONTOLOGY.toString(), pEID,
//                                ont.peek().get("ID").asInt(), RelTypes.ONTOLOGY);
//                    }
//
//                    // 1.2. Insert the cellular locations
//                    for (CellularLocation c: p.getCellularLocation()) {
//                        StatementResult cellLoc = getOrCreateCellularLocationNode(tx, parseCellularLocation(c));
//                        addRelationship(tx, peLabel, NodeTypes.CELLULAR_LOCATION.toString(), pEID,
//                                cellLoc.peek().get("ID").asInt(), RelTypes.CELLULAR_LOCATION);
//                    }
//
//                    // 1.3. Insert the Xrefs
//                    for (Xref xref: p.getXrefs()) {
//                        StatementResult xr = getOrCreateNode(tx, NodeTypes.XREF.toString(), parseXref(xref));
//                        addRelationship(tx, peLabel, NodeTypes.XREF.toString(), pEID,
//                                xr.peek().get("ID").asInt(), RelTypes.XREF);
//                    }
//                } else if (node.getType() == Node.Type.XREF) {
//                    getOrCreateNode(tx, NodeTypes.XREF.toString(), parseXref((Xref) node));
//                } else {
//                    StringBuilder labels = new StringBuilder(node.getType().toString());
////                    for (org.opencb.bionetdb.core.models.Node.Type subtype: node.getSubtypes()) {
////                        tags.append(":").append(subtype.toString());
////                    }
//                    getOrCreateNode(tx, labels.toString(), (ObjectMap) node.getAttributes());
//                }
//                return 1;
//            });
//        }
//
//        for (Node node: nodeList) {
//            session.writeTransaction(tx -> {
//                if (Node.isPhysicalEntity(node)
//                        && node.getType() != Node.Type.GENE) {
//                    PhysicalEntity p = (PhysicalEntity) node;
//                    // 2. Insert the existing relationships between Physical Entities
//                    if (p.getComponentOfComplex().size() > 0) {
//                        StatementResult peNode = getNode(tx, NodeTypes.PHYSICAL_ENTITY.toString(), parsePhysicalEntity(p));
//                        if (peNode == null) {
//                            return 0;
////                        throw new BioNetDBException("PHYSICAL_ENTITY \"" + p.getId()
////                                + "\" is not properly inserted in the database.");
//                        }
//                        for (String complexID : p.getComponentOfComplex()) {
//                            StatementResult complexNode = getNode(tx, NodeTypes.PHYSICAL_ENTITY.toString(),
//                                    new ObjectMap("id", complexID));
//                            if (complexNode == null) {
//                                return 0;
////                            throw new BioNetDBException("PHYSICAL_ENTITY:COMPLEX \"" + complexID
////                                    + "\": is not properly inserted");
//                            }
//                            if (complexNode.peek().get("LABELS").toString().contains(PhysicalEntity.Type.COMPLEX.toString())) {
//                                String peLabel = NodeTypes.PHYSICAL_ENTITY + ":" + p.getType();
//                                addRelationship(tx, peLabel, NodeTypes.PHYSICAL_ENTITY + ":" + PhysicalEntity.Type.COMPLEX,
//                                        peNode.peek().get("ID").asInt(), complexNode.peek().get("ID").asInt(),
//                                        RelTypes.COMPONENTOFCOMPLEX);
//                            } else {
//                                return 0;
////                            throw new BioNetDBException("Relation 'COMPONENTOFCOMPLEX' cannot be created "
////                                    + "because the destiny node is of type \""
////                                    + complexNode.peek().get("LABELS").toString()
////                                    + "\" Check Physical Entity \"" + complexID + "\"");
//                            }
//                        }
//                    }
//                }
//                return 1;
//            });
//        }
//        session.close();
//    }
//
//    /**
//     * Insert all the relationships into the Neo4J database.
//     *
//     * @param relationList List containing all the interactions to be inserted in the database
//     * @param queryOptions     Additional params for the query
//     */
//    private void insertRelationships(List<Relation> relationList, QueryOptions queryOptions) {
//        Session session = this.driver.session();
//
//        // 1. Insert all interactions as nodes
//        for (Relation r: relationList) {
//            session.writeTransaction(tx -> {
//                if (Relation.isInteraction(r)) {
//                    Interaction i = (Interaction) r;
//                    String interactionLabel = NodeTypes.INTERACTION + ":" + i.getType();
//                    getOrCreateNode(tx, interactionLabel, parseInteraction(i));
//                } else {
//                    StatementResult orig = getNode(tx, r.getOriginType(), new ObjectMap("id", r.getOriginId()));
//                    StatementResult dest = getNode(tx, r.getDestType(), new ObjectMap("id", r.getDestId()));
//                    addRelationship(tx, r.getOriginType(), r.getDestType(),
//                            orig.peek().get("ID").asInt(), dest.peek().get("ID").asInt(),
//                            RelTypes.valueOf(r.getType().toString()));
//
//                }
//                return 1;
//            });
//        }
//
//        // 2. Insert the interactions
//        for (Relation r: relationList) {
//            session.writeTransaction(tx -> {
//                if (Relation.isInteraction(r)) {
//                    Interaction i = (Interaction) r;
//                    String interactionLabel = NodeTypes.INTERACTION + ":" + i.getType();
//                    StatementResult interaction = getNode(tx, interactionLabel, new ObjectMap("id", i.getId()));
//                    int interactionID = interaction.peek().get("ID").asInt();
//
//                    switch (i.getType()) {
//                        case REACTION:
//                            Reaction myreaction = (Reaction) i;
//                            for (String myId : myreaction.getReactants()) {
//                                StatementResult reactant = getNode(tx, NodeTypes.PHYSICAL_ENTITY.toString(),
//                                        new ObjectMap("id", myId));
//                                addRelationship(tx, concatenateLabels(reactant.peek().get("LABELS")),
//                                        interactionLabel, reactant.peek().get("ID").asInt(),
//                                        interactionID, RelTypes.REACTANT);
//                            }
//
//                            for (String myId : myreaction.getProducts()) {
//                                StatementResult product = getNode(tx, NodeTypes.PHYSICAL_ENTITY.toString(),
//                                        new ObjectMap("id", myId));
//                                addRelationship(tx, interactionLabel, concatenateLabels(product.peek().get("LABELS")),
//                                        interactionID, product.peek().get("ID").asInt(), RelTypes.PRODUCT);
//                            }
//                            break;
//                        case CATALYSIS:
//                            Catalysis mycatalysis = (Catalysis) i;
//                            for (String myId : mycatalysis.getControllers()) {
//                                StatementResult reactant = getNode(tx, NodeTypes.PHYSICAL_ENTITY.toString(),
//                                        new ObjectMap("id", myId));
//                                addRelationship(tx, concatenateLabels(reactant.peek().get("LABELS")),
//                                        interactionLabel, reactant.peek().get("ID").asInt(),
//                                        interactionID, RelTypes.CONTROLLER);
//                            }
//
//                            for (String myId : mycatalysis.getControlledProcesses()) {
//                                StatementResult product = getNode(tx, NodeTypes.INTERACTION.toString(),
//                                        new ObjectMap("id", myId));
//                                addRelationship(tx, interactionLabel, concatenateLabels(product.peek().get("LABELS")),
//                                        interactionID, product.peek().get("ID").asInt(), RelTypes.CONTROLLED);
//                            }
//                            break;
//                        case REGULATION:
//                            Regulation myregulation = (Regulation) i;
//                            for (String myId : myregulation.getControllers()) {
//                                StatementResult reactant = getNode(tx, NodeTypes.PHYSICAL_ENTITY.toString(),
//                                        new ObjectMap("id", myId));
//                                addRelationship(tx, concatenateLabels(reactant.peek().get("LABELS")),
//                                        interactionLabel, reactant.peek().get("ID").asInt(),
//                                        interactionID, RelTypes.CONTROLLER);
//                            }
//
//                            for (String myId : myregulation.getControlledProcesses()) {
//                                StatementResult product = getNode(tx, NodeTypes.INTERACTION.toString(),
//                                        new ObjectMap("id", myId));
//                                addRelationship(tx, interactionLabel, concatenateLabels(product.peek().get("LABELS")),
//                                        interactionID, product.peek().get("ID").asInt(), RelTypes.CONTROLLED);
//                            }
//                            break;
//                        default:
//                            break;
//                    }
//                }
//                return 1;
//            });
//        }
//        session.close();
//    }
//
//    private StatementResult getNode(Transaction tx, String label, ObjectMap properties) {
//        // Gathering properties of the node to create a cypher string with them
//        List<String> props = new ArrayList<>();
//        for (String key : properties.keySet()) {
//            props.add(key + ":\"" + properties.getString(key) + "\"");
//        }
//        String propsJoined = "{" + String.join(",", props) + "}";
//        // Getting the desired node
//        return tx.run("MATCH (n:" + label + " " + propsJoined + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS");
//    }
//
//    private StatementResult updateNode(Transaction tx, String label, ObjectMap properties, ObjectMap newProperties) {
//        // Gathering properties of the node to create a cypher string with them
//        List<String> props = new ArrayList<>();
//        for (String key: properties.keySet()) {
//            props.add(key + ":\"" + properties.getString(key) + "\"");
//        }
//        String propsJoined = "{" + String.join(",", props) + "}";
//
//        props.clear();
//        for (String key: newProperties.keySet()) {
//            props.add("n." + key + "=\"" + newProperties.getString(key) + "\"");
//        }
//        String setJoined = "SET " + String.join(",", props);
//
//        // Getting the desired node
//        String cypher = "MATCH (n:" + label + " " + propsJoined + ") " + setJoined + " RETURN ID(n) AS ID, LABELS(n) AS LABELS";
//        //cypher = cypher.replace("\"\"", "\"");
//        return tx.run(cypher);
//    }
//
//    private StatementResult createNode(Transaction tx, String label, ObjectMap properties) {
//        // Gathering properties of the node to create a cypher string with them
//        List<String> props = new ArrayList<>();
//        for (String key : properties.keySet()) {
//            props.add(key + ":\"" + properties.getString(key) + "\"");
//        }
//        String propsJoined = "{" + String.join(",", props) + "}";
//        // Creating the desired node
//        return tx.run("CREATE (n:" + label + " " + propsJoined + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS");
//    }
//
//    /**
//     * @param tx          Transaction
//     * @param label:      Label of the node
//     * @param properties: Map containing all the properties to be added. Key "id" must be among all the possible keys.
//     * @return Node that has been created.
//     */
//    private StatementResult getOrCreateNode(Transaction tx, String label, ObjectMap properties) {
//        // Gathering properties of the node to create a cypher string with them
//        ObjectMap update = null;
//        StringBuilder props = new StringBuilder("");
//        if (MapUtils.isNotEmpty(properties)) {
//            props.append("{");
//            boolean first = true;
//            for (String key : properties.keySet()) {
//                if (key.equals("_update")) {
//                    update = (ObjectMap) properties.get(key);
//                    continue;
//                }
//                if (!first) {
//                    props.append(",");
//                }
//                props.append(key).append(":$").append(key);
//                first = false;
//            }
//            props.append("}");
//        }
//
//        if (update != null) {
//            properties.remove("_update");
//            return updateNode(tx, label, properties, update);
//        } else {
//            // Getting the desired node or creating it if it does not exists
//            return tx.run("MERGE (n:" + label + " " + props + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS",
//                    properties);
//        }
//    }
//
//    private StatementResult getOrCreateNode00(Transaction tx, String label, ObjectMap properties) {
//        // Gathering properties of the node to create a cypher string with them
//        List<String> props = new ArrayList<>();
//        for (String key : properties.keySet()) {
//            //props.add(key + ":\"" + properties.getString(key) + "\"");
//            props.add(key + ":\"" + properties.getString(key)
//                    .replace("\"", ",")
//                    .replace("\\", "|") + "\"");
//        }
//        String propsJoined = "{" + String.join(",", props) + "}";
//
//        // Getting the desired node or creating it if it does not exists
//        return tx.run("MERGE (n:" + label + " " + propsJoined + ") RETURN ID(n) AS ID, LABELS(n) AS LABELS");
//    }
//
//    /**
//     * Checks if the cellular location given in properties is already in database. If not, it creates it and
//     * returns the node.
//     *
//     * @param tx          Transaction
//     * @param properties: Map containing all the properties to be added. Key "id" must be among all the possible keys.
//     * @return Node that has been created.
//     */
//    private StatementResult getOrCreateCellularLocationNode(Transaction tx, ObjectMap properties) {
//        StatementResult cellLoc = getOrCreateNode(tx, NodeTypes.CELLULAR_LOCATION.toString(),
//                new ObjectMap("name", properties.get("name")));
//        // gets or creates ontology node
//        if (properties.containsKey("ontologies")) {
//            for (ObjectMap myOntology : (List<ObjectMap>) properties.get("ontologies")) {
//                StatementResult ont = getOrCreateNode(tx, NodeTypes.ONTOLOGY.toString(), myOntology);
//                addRelationship(tx, NodeTypes.CELLULAR_LOCATION.toString(), NodeTypes.ONTOLOGY.toString(),
//                        cellLoc.peek().get("ID").asInt(), ont.peek().get("ID").asInt(),
//                        RelTypes.CELLOC_ONTOLOGY);
//            }
//        }
//        return cellLoc;
//    }
//
//    private StatementResult getOrCreateConsequenceTypeNode(Transaction tx, ObjectMap properties) {
//
//        // gets the sequence ontology properties
//        List<ObjectMap> soProperties = null;
//        if (properties.containsKey("so")) {
//            soProperties = (List<ObjectMap>) properties.get("so");
//            properties.remove("so");
//        }
//
//        // gets the biotype property
//        String biotype = null;
//        if (properties.containsKey("biotype")) {
//            biotype = (String) properties.get("biotype");
//            properties.remove("biotype");
//        }
//
//        // gets the genes property
//        List<String> genes = null;
//        if (properties.containsKey("genes")) {
//            genes = (List<String>) properties.get("genes");
//            properties.remove("genes");
//        }
//
//        StatementResult csNode = getOrCreateNode(tx, NodeTypes.CONSEQUENCE_TYPE.toString(), properties);
//        if (soProperties != null) {
//            for (ObjectMap so: soProperties) {
//                StatementResult soNode = getOrCreateNode(tx, NodeTypes.SEQUENCE_ONTOLOGY.toString(), so);
//                addRelationship(tx, NodeTypes.CONSEQUENCE_TYPE.toString(), NodeTypes.SEQUENCE_ONTOLOGY.toString(),
//                        csNode.peek().get("ID").asInt(), soNode.peek().get("ID").asInt(),
//                        RelTypes.SEQUENCE_ONTOLOGY);
//            }
//        }
//        if (biotype != null) {
//            StatementResult biotypeNode = getOrCreateNode(tx, NodeTypes.BIOTYPE.toString(), new ObjectMap("name", biotype));
//            addRelationship(tx, NodeTypes.CONSEQUENCE_TYPE.toString(), NodeTypes.BIOTYPE.toString(),
//                    csNode.peek().get("ID").asInt(), biotypeNode.peek().get("ID").asInt(),
//                    RelTypes.BIOTYPE);
//        }
//        if  (ListUtils.isNotEmpty(genes)) {
//            StatementResult geneNode = getOrCreateNode(tx, PhysicalEntity.Type.GENE.toString(),
//                    new ObjectMap("name", genes.get(0)));
//            addRelationship(tx, NodeTypes.CONSEQUENCE_TYPE.toString(), PhysicalEntity.Type.GENE.toString(),
//                    csNode.peek().get("ID").asInt(), geneNode.peek().get("ID").asInt(),
//                    RelTypes.IN_GENE);
//            for (String gene: genes) {
//                StatementResult xrefNode = getOrCreateNode(tx, NodeTypes.XREF.toString(), new ObjectMap("name", gene));
//                addRelationship(tx, NodeTypes.XREF.toString(), PhysicalEntity.Type.GENE.toString(),
//                        xrefNode.peek().get("ID").asInt(), geneNode.peek().get("ID").asInt(),
//                        RelTypes.XREF);
//            }
//        }
//        return csNode;
//    }
//
//    /**
//     * The function will create an interaction between two nodes if the relation does not exist.
//     *
//     * @param tx            Transaction
//     * @param originID      Node ID from which we want to create the interaction
//     * @param destinationID Destination node ID
//     * @param relationType  Type of relationship between nodes
//     */
//    private void addRelationship(Transaction tx, String labelOri, String labelDest, int originID,
//                                 int destinationID, RelTypes relationType) {
//        StringBuilder statementTemplate = new StringBuilder();
//        statementTemplate.append("MATCH (o:").append(labelOri).append(") WHERE ID(o) = $originId")
//                .append(" MATCH (d:").append(labelDest).append(") WHERE ID(d) = $destinationID")
//                .append(" MERGE (o)-[:").append(relationType).append("]->(d)");
//        tx.run(statementTemplate.toString(), parameters("originId", originID, "destinationID", destinationID));
//    }
//
//    //"CREATE (a:Person {name: $name})", parameters( "name", name ) );
//
//    /**
//     * This method will be called every time we consider that two existing nodes are the same and should be merged.
//     *
//     * @param node1
//     * @param node2
//     */
//    // TODO: Maybe we would have to add the type of nodes we want to merge... Now it is done considering they are PE
//    private void mergeNodes(Node node1, Node node2) {
///*        Node myNewNode = null;
//        // TODO: 1. Merge the basic information from both nodes into this one.
//        // 2. Destroy the relationships present in both nodes and apply them to the new node.
//        // TODO: Check expression data
//        // TODO: What would happen if two different nodes have different expression data....??
//        // TODO: What if both of them have expression for the same tissues, but for different timeseries??
//        // Expression data
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.TISSUE, Direction.OUTGOING)) {
//            addRelationship(myNewNode, nodeAux, RelTypes.TISSUE);
//        }
//        // Ontologies
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.ONTOLOGY, Direction.OUTGOING)) {
//            addRelationship(myNewNode, nodeAux, RelTypes.ONTOLOGY);
//        }
//        // Xrefs
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.XREF, Direction.OUTGOING)) {
//            addRelationship(myNewNode, nodeAux, RelTypes.XREF);
//        }
//        // Reactant outgoing
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.REACTANT, Direction.OUTGOING)) {
//            addRelationship(myNewNode, nodeAux, RelTypes.REACTANT);
//        }
//        // Reactant incoming
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.REACTANT, Direction.INCOMING)) {
//            addRelationship(nodeAux, myNewNode, RelTypes.REACTANT);
//        }
//        // Controller
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.CONTROLLER, Direction.OUTGOING)) {
//            addRelationship(myNewNode, nodeAux, RelTypes.CONTROLLER);
//        }
//        // Controlled
//        for (Node nodeAux : getUniqueNodes(node1, node2, RelTypes.CONTROLLED, Direction.INCOMING)) {
//            addRelationship(nodeAux, myNewNode, RelTypes.CONTROLLED);
//        }
//        // TODO: This will launch an exception if the nodes still contain relationships
//        node1.delete();
//        node2.delete();
//*/
//    }
//
//    /**
//     * Method necessary to merge nodes. This method will check for a pair of nodes, the nodes that can be achieved
//     * given the same relationship and direction and return the set comprised by the two of them.
//     * All the relationships from node1 and node2 to the set returned by the method will be removed from the database.
//     *
//     * @param node1    Node
//     * @param node2    Node
//     * @param relation Relation to follow
//     */
//    private Set<Node> getUniqueNodes(Node node1, Node node2, RelTypes relation) {
//        Set<Node> myUniqueNodes = new HashSet<>();
//        // TODO: Be sure that this set only stores non-repeated nodes
////        for (Relation relationShip : node1.getRelations(relation, direction)) {
////            myUniqueNodes.add(relationShip.getOtherNode(node1));
////            relationShip.delete();
////        }
////        for (Relation relationShip : node2.getRelations(relation, direction)) {
////            myUniqueNodes.add(relationShip.getOtherNode(node2));
////            relationShip.delete();
////        }
//        return myUniqueNodes;
//    }
//
//    /**
//     * Parses the Node node into an ontology bean.
//     *
//     * @param node
//     * @return
//     * @throws BioNetDBException
//     */
//    private Ontology node2Ontology(Node node) throws BioNetDBException {
//        Ontology myOntology = new Ontology();
////        if (node.hasProperty("source")) {
////            myOntology.setSource((String) node.getProperty("source"));
////        }
////        if (node.hasProperty("sourceVersion")) {
////            myOntology.setSource((String) node.getProperty("sourceVersion"));
////        }
////        if (node.hasProperty("id")) {
////            myOntology.setSource((String) node.getProperty("id"));
////        }
////        if (node.hasProperty("idVersion")) {
////            myOntology.setSource((String) node.getProperty("idVersion"));
////        }
////        if (node.hasProperty("name")) {
////            myOntology.setSource((String) node.getProperty("name"));
////        }
////        if (node.hasProperty("description")) {
////            myOntology.setSource((String) node.getProperty("description"));
////        }
//        return myOntology;
//    }
//
//
////    private CellularLocation node2CellularLocation(Node node) throws BioNetDBException {
////        CellularLocation myCellularLocation = new CellularLocation();
////        if (node.hasProperty("id")) {
////            myCellularLocation.setName((String) node.getProperty("id"));
////        }
////        if (node.hasRelationship(RelTypes.CELLOC_ONTOLOGY, Direction.OUTGOING)) {
////            List<Ontology> myOntologies = new ArrayList<>();
////            for (Relation myRelationship : node.getRelations(RelTypes.CELLOC_ONTOLOGY, Direction.OUTGOING)) {
////                myOntologies.add(node2Ontology(myRelationship.getEndNode()));
////            }
////            myCellularLocation.setOntologies(myOntologies);
////        }
////        return myCellularLocation;
////    }
//
//
////    private PhysicalEntity node2PhysicalEntity(Node node) throws BioNetDBException {
////        PhysicalEntity p = null;
////        switch ((PhysicalEntity.Type) node.getProperty("type")) {
////            case COMPLEX:
////                p = new Complex();
////                break;
////            case UNDEFINED:
////                p = new Undefined();
////                break;
////            case PROTEIN:
////                p = new Protein();
////                break;
////            case DNA:
////                p = new Dna();
////                break;
////            case RNA:
////                p = new Rna();
////                break;
////            case SMALL_MOLECULE:
////                p = new SmallMolecule();
////                break;
////            default:
////                break;
////        }
////        if (p == null) {
////            throw new BioNetDBException("The node intended to be parsed to a Physical Entity seems not to be a proper"
////                    + "Physical Entity node");
////        } else {
////            p.setId((String) node.getProperty("id"));
////            p.setName((String) node.getProperty("name"));
////            p.setDescription((List<String>) node.getProperty("description"));
////            p.setSource((List<String>) node.getProperty("source"));
////
////            if (node.hasRelationship(RelTypes.ONTOLOGY)) {
////                List<Ontology> ontologyList = new ArrayList<>();
////                for (Relation relationship : node.getRelations(Direction.OUTGOING, RelTypes.ONTOLOGY)) {
////                    Node ontologyNode = relationship.getEndNode();
////                    ontologyList.add(node2Ontology(ontologyNode));
////                }
////                p.setOntologies(ontologyList);
////            }
////
////            if (node.hasRelationship(RelTypes.CELLULAR_LOCATION)) {
////                List<CellularLocation> cellularLocationList = new ArrayList<>();
////                for (Relation relationship : node.getRelations(Direction.OUTGOING, RelTypes.CELLULAR_LOCATION)) {
////                    Node cellularLocationNode = relationship.getEndNode();
////                    cellularLocationList.add(node2CellularLocation(cellularLocationNode));
////                }
////                p.setCellularLocation(cellularLocationList);
////            }
////        }
////
////        return p;
////    }
//
//    @Override
//    public QueryResult getNodes(Query query, QueryOptions queryOptions) throws BioNetDBException {
//        Session session = this.driver.session();
//
//        long startTime = System.currentTimeMillis();
//        //TODO: improve
//        String myQuery;
//        if (query.containsKey(NetworkQueryParams.SCRIPT.key())) {
//            myQuery = query.getString(NetworkQueryParams.SCRIPT.key());
//        } else {
//            String nodeName = "n";
//            myQuery = "MATCH " + Neo4JQueryParser.parse(nodeName, query, queryOptions) + " RETURN " + nodeName;
//        }
//        System.out.println("Query: " + myQuery);
//        long stopTime = System.currentTimeMillis();
//        StatementResult run = session.run(myQuery);
//        List<Node> nodes = new ArrayList<>();
//        while (run.hasNext()) {
//            Map<String, Object> map = run.next().asMap();
//            for (String key: map.keySet()) {
//                org.neo4j.driver.v1.types.Node neoNode = (org.neo4j.driver.v1.types.Node) map.get(key);
//                Node node = new Node();
//                node.setId(neoNode.get("id").asString());
//                node.setName(neoNode.get("name").asString());
//                Iterator<String> iterator = neoNode.labels().iterator();
//                // TODO: improve type manangement
//                String firstType = null;
//                String secondType = null;
//                while (iterator.hasNext()) {
//                    if (firstType == null) {
//                        firstType = iterator.next();
//                    } else if (secondType == null) {
//                        secondType = iterator.next();
//                        break;
//                    }
//                }
//                if (secondType != null) {
//                    node.setType(Node.Type.valueOf(secondType));
//                } else if (firstType != null) {
//                    node.setType(Node.Type.valueOf(firstType));
//                }
//                for (String k: neoNode.keys()) {
//                    node.addAttribute(k, neoNode.get(k).asString());
//                }
//                nodes.add(node);
//            }
//        }
//        int time = (int) (stopTime - startTime) / 1000;
//
//        session.close();
//        return new QueryResult("get", time, 0, 0, null, null, nodes);
//    }
//
//    @Override
//    public QueryResult getNodes(Query queryN, Query queryM, QueryOptions queryOptions) throws BioNetDBException {
//        Session session = this.driver.session();
//
//        long startTime = System.currentTimeMillis();
//        String nQuery = Neo4JQueryParser.parse("n", queryN, queryOptions);
//        String mQuery = Neo4JQueryParser.parse("m", queryM, queryOptions);
//
//        // MATCH (n)-[:XREF]->(nx:XREF) , (m)-[:XREF]->(mx:XREF) , (n)-[:REACTANT|:PRODUCT*..2]-(m)
//        //  WHERE nx.id IN ["P40343"] AND mx.id IN ["5732871"] RETURN n
//
//        // Creating relationship between nodes n and m
//        StringBuilder relQuery = new StringBuilder("(n)-[");
//        if (queryOptions.containsKey(NetworkQueryParams.REL_TYPE.key())) {
//            relQuery.append(":").append(queryOptions.getString(NetworkQueryParams.REL_TYPE.key())
//                    .replace(",", "|:"));
//        }
//        if (queryOptions.containsKey(NetworkQueryParams.JUMPS.key())) {
//            relQuery.append("*..").append(queryOptions.getInt(NetworkQueryParams.JUMPS.key()));
//        }
//        relQuery.append("]-(m) ");
//
//        StringBuilder myQuery = new StringBuilder();
//
//        // Creating the MATCH part in cypher query
//        List<String> myMatch = new ArrayList<>();
//        myMatch.add(nQuery.split("WHERE")[0]);
//        myMatch.add(mQuery.split("WHERE")[0]);
//        myMatch.add(relQuery.toString());
//        myMatch.removeAll(Arrays.asList("", null));
//        myQuery.append("MATCH ").append(StringUtils.join(myMatch, " , "));
//
//        // Creating the WHERE part in cypher query
//        List<String> myWhere = new ArrayList<>();
//        if (nQuery.contains("WHERE")) {
//            myWhere.add(nQuery.split("WHERE")[1]);
//        }
//        if (mQuery.contains("WHERE")) {
//            myWhere.add(mQuery.split("WHERE")[1]);
//        }
//        if (myWhere.size() > 0) {
//            myQuery.append(" WHERE ").append(StringUtils.join(myWhere, " AND "));
//        }
//
//        // Getting the nodes that should be returned
//        StringBuilder ret = new StringBuilder(" RETURN n");
//        Pattern pattern = Pattern.compile("\\((m.?):[A-Z_]+\\)");
//        Matcher m = pattern.matcher(mQuery.split("WHERE")[0]);
//        while (m.find()) {
//            ret.append(",").append(m.group(1));
//        }
//        myQuery.append(ret.toString());
//
//        System.out.println("myQuery: " + myQuery.toString());
//        long stopTime = System.currentTimeMillis();
//        StatementResult run = session.run(myQuery.toString());
//        while (run.hasNext()) {
//            System.out.println(run.next().asMap());
//        }
//        int time = (int) (stopTime - startTime) / 1000;
//
//        session.close();
//        return new QueryResult("get", time, 0, 0, null, null, Arrays.asList(new Network()));
//    }
//
//    @Override
//    public QueryResult getAnnotations(Query query, String annotateField) {
//        Session session = this.driver.session();
//
//        QueryOptions queryOptions = new QueryOptions("include", annotateField);
//        long startTime = System.currentTimeMillis();
//        StringBuilder myQuery = new StringBuilder();
//        List<String> idList = new ArrayList<>();
//        try {
//            // MATCH (x:PHYSICAL_ENTITY) WHERE x.name IN ["x"] MATCH (y:PHYSICAL_ENTITY) WHERE y.name IN ["y"] WITH x,y
//            // MERGE (c:PHYSICAL_ENTITY {name:"c"}) MERGE (x)-[r:XREF]->(c)
//            // MERGE (b:PHYSICAL_ENTITY {name:"b"}) MERGE (y)-[r1:XREF]->(b) RETURN x,y
//            idList = Arrays.asList(query.getString("id").split(","));
//            for (String anIdList : idList) {
//                myQuery.append("MATCH ").append(Neo4JQueryParser.parse(anIdList, query, queryOptions)).append(" ");
//            }
//            myQuery.append("WITH ").append(StringUtils.join(idList, ","));
//        } catch (BioNetDBException e) {
//            e.printStackTrace();
//        }
//
//        StringBuilder mergeQuery = new StringBuilder(" ");
//        try {
//            QueryResponse<Gene> queryResponse =
//                    cellBaseClient.getGeneClient().get(Collections.singletonList(query.getString("id")), queryOptions);
//            //  MERGE (c:PHYSICAL_ENTITY {name:"c"}) MERGE (x)-[r:XREF]->(c) MERGE (b:PHYSICAL_ENTITY {name:"b"}) MERGE (y)-[r1:XREF]->(b)
//            for (int i = 0; i < queryResponse.getResponse().size(); i++) {
//                // Iterate over the results and add node per result
//                for (int j = 0; j < queryResponse.getResponse().get(i).getResult().get(0).getTranscripts().size(); j++) {
//                    for (int k = 0; k < queryResponse.getResponse().get(i).getResult().get(0)
//                            .getTranscripts().get(j).getXrefs().size(); k++) {
//                        // Take care of the label of node
//                        mergeQuery.append("MERGE (n").append(i).append(j).append(k).append(":").append("XREF").append("{");
//                        // properties to add on the node goes here
//                        mergeQuery.append("}) ").append("MERGE (").append(idList.get(i)).append(")-[r").append(i).append(":")
//                                .append(annotateField.toUpperCase()).append("]->(n").append(i).append(j).append(k).append(") ");
//
//                    }
//                }
//            }
//            myQuery.append(mergeQuery).append("RETURN ").append(StringUtils.join(idList, ","));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("Query: " + myQuery);
//        long stopTime = System.currentTimeMillis();
//        StatementResult run = session.run(myQuery.toString());
//        while (run.hasNext()) {
//            System.out.println(run.next().asMap());
//        }
//        int time = (int) (stopTime - startTime) / 1000;
//
//        session.close();
//        return new QueryResult("get", time, 0, 0, null, null, Arrays.asList(new Network()));
//    }
//
//    private int getTotalNodes() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n) RETURN count(n) AS count").peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n) RETURN count(n) AS count").peek().get("count").asInt();
//    }
//
//    private int getTotalRelationships() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH ()-[r]-() RETURN count(r) AS count").peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH ()-[r]-() RETURN count(r) AS count").peek().get("count").asInt();
//    }
//
//    private ObjectMap getTotalPhysicalEntities() {
//        Session session = this.driver.session();
//
//        ObjectMap myResult = new ObjectMap();
//        myResult.put("undefined", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.UNDEFINED + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("protein", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.PROTEIN + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("gene", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.GENE + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("variant", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.VARIANT + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("dna", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.DNA + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("rna", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.RNA + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("complex", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.COMPLEX + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        myResult.put("small_molecule", session.run(("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY + ":"
//                + PhysicalEntity.Type.SMALL_MOLECULE + ") RETURN count(n) AS count")).peek().get("count").asInt());
//        int total = 0;
//        for (String key : myResult.keySet()) {
//            total += (int) myResult.get(key);
//        }
//        myResult.put("totalPE", total);
//
//        session.close();
//        return myResult;
//    }
//
//    private int getTotalXrefNodes() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n:" + NodeTypes.XREF + ") RETURN count(n) AS count")
//                .peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n:" + NodeTypes.XREF + ") RETURN count(n) AS count")
////                .peek().get("count").asInt();
//    }
//
//    private int getTotalXrefRelationships() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n:"
//                + NodeTypes.PHYSICAL_ENTITY + ")-[r:" + RelTypes.XREF + "]->(m:"
//                + NodeTypes.XREF + ") RETURN count(r) AS count").peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n:"
////                + NodeTypes.PHYSICAL_ENTITY + ")-[r:" + RelTypes.XREF + "]->(m:"
////                + NodeTypes.XREF + ") RETURN count(r) AS count").peek().get("count").asInt();
//    }
//
//    private int getTotalOntologyNodes() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n:" + NodeTypes.ONTOLOGY + ") RETURN count(n) AS count")
//                .peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n:" + NodeTypes.ONTOLOGY + ") RETURN count(n) AS count")
////                .peek().get("count").asInt();
//    }
//
//    private int getTotalOntologyRelationships() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n)-[r:" + RelTypes.ONTOLOGY + "|" + RelTypes.CELLOC_ONTOLOGY
//                + "]->(m:" + NodeTypes.ONTOLOGY + ") RETURN count(r) AS count").peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n)-[r:" + RelTypes.ONTOLOGY + "|" + RelTypes.CELLOC_ONTOLOGY
////                + "]->(m:" + NodeTypes.ONTOLOGY + ") RETURN count(r) AS count").peek().get("count").asInt();
//    }
//
//    private int getTotalCelLocationNodes() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n:" + NodeTypes.CELLULAR_LOCATION + ") RETURN count(n) AS count")
//                .peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n:" + NodeTypes.CELLULAR_LOCATION + ") RETURN count(n) AS count")
////                .peek().get("count").asInt();
//    }
//
//    private int getTotalCelLocationRelationships() {
//        Session session = this.driver.session();
//        int count = session.run("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY
//                + ")-[r:" + RelTypes.CELLULAR_LOCATION + "]->(m:" + NodeTypes.CELLULAR_LOCATION
//                + ") RETURN count(r) AS count").peek().get("count").asInt();
//        session.close();
//        return count;
////        return this.session.run("MATCH (n:" + NodeTypes.PHYSICAL_ENTITY
////                + ")-[r:" + RelTypes.CELLULAR_LOCATION + "]->(m:" + NodeTypes.CELLULAR_LOCATION
////                + ") RETURN count(r) AS count").peek().get("count").asInt();
//    }
//
//    @Override
//    public QueryResult getNetwork(Query query, QueryOptions queryOptions) throws BioNetDBException {
//        Network network = new Network();
//        Session session = this.driver.session();
//
//        long startTime = System.currentTimeMillis();
//        //TODO: improve
//        String myQuery;
//        if (query.containsKey(NetworkQueryParams.SCRIPT.key())) {
//            myQuery = query.getString(NetworkQueryParams.SCRIPT.key());
//        } else {
//            throw new BioNetDBException("Not implemented yet! So far, network only can be got from a cypher statement");
//        }
//        System.out.println("Query: " + myQuery);
//
//        Map<String, org.neo4j.driver.v1.types.Node> neoNodeMap = new HashMap<>();
//
//        long stopTime = System.currentTimeMillis();
//        StatementResult run = session.run(myQuery);
//        int numRecords = 0;
//        while (run.hasNext()) {
//            Record record = run.next();
//            System.out.println((++numRecords) + ": record size = " + record.size());
//
//            for (int i = 0; i < record.size(); i++) {
//                System.out.println("\t" + i + ", " + record.get(i).type() + ", " + record.keys().get(i));
//
//                if (record.get(i).hasType(TYPE_SYSTEM.PATH())) {
//                    Path neoPath = record.get(i).asPath();
//                    // Neo4j node management
//                    for (org.neo4j.driver.v1.types.Node neoNode: neoPath.nodes()) {
//                        neoNodeMap.put(String.valueOf(neoNode.id()), neoNode);
//                        Node node = toNode(neoNode);
//                        network.setNode(node);
//                    }
//                    // Neo4j relathinship management
//                    for (org.neo4j.driver.v1.types.Relationship neoRelation: neoPath.relationships()) {
//                        Relation relation = toRelationship(neoRelation, neoNodeMap);
//                        network.setRelationship(relation);
//                    }
//                }
//            }
//        }
//        int time = (int) (stopTime - startTime) / 1000;
//
//        session.close();
//        return new QueryResult("get", time, 0, 0, null, null, Arrays.asList(network));
//    }
//
//    private String getNeo4jNodeLabel(org.neo4j.driver.v1.types.Node neoNode) {
//        String label = null;
//        try {
//            Iterator<String> iterator = neoNode.labels().iterator();
//            while (iterator.hasNext()) {
//                label = iterator.next();
//            }
//        } catch (NullPointerException e) {
//            System.out.println(neoNode.toString());
//        }
//        return label;
//    }
//
//    private Node toNode(org.neo4j.driver.v1.types.Node neoNode) {
//        // TODO: improve label/type manangement
//        Node node;
//        String label = getNeo4jNodeLabel(neoNode);
//        if (neoNode.hasLabel(Node.Type.XREF.name())) {
//
//            node = new Xref(neoNode.get("source").asString(), neoNode.get("sourceVersion").asString(),
//                    neoNode.get("id").asString(), neoNode.get("idVersion").asString());
//        } else {
//            node = new Node(neoNode.get("id").asString(), neoNode.get("name").asString(),
//                    Node.Type.valueOf(getNeo4jNodeLabel(neoNode)));
//        }
//        for (String key: neoNode.keys()) {
//            node.addAttribute(key, neoNode.get(key).asString());
//        }
//        return node;
//    }
//
//    private Relation toRelationship(org.neo4j.driver.v1.types.Relationship neoRelation,
//                                    Map<String, org.neo4j.driver.v1.types.Node> neoNodeMap) {
//        Relation relation = new Relation();
//        relation.setId(String.valueOf(neoRelation.id()));
//        relation.setOriginId(neoNodeMap.get(String.valueOf(neoRelation.startNodeId())).get("id").asString());
//        relation.setOriginType(getNeo4jNodeLabel(neoNodeMap.get(String.valueOf(neoRelation.startNodeId()))));
//        relation.setDestId(neoNodeMap.get(String.valueOf(neoRelation.endNodeId())).get("id").asString());
//        relation.setDestType(getNeo4jNodeLabel(neoNodeMap.get(String.valueOf(neoRelation.endNodeId()))));
//        relation.setType(Relation.Type.valueOf(neoRelation.type().toString()));
//        System.out.println("NeoRelation: " + neoRelation.toString());
////                        //neoRelation.
//        for (String key : neoRelation.keys()) {
//            relation.addAttribute(key, neoRelation.get(key).asString());
//        }
//        return relation;
//    }
//
//    @Override
//    public QueryResult getSummaryStats(Query query, QueryOptions queryOptions) {
//        long startTime = System.currentTimeMillis();
//        ObjectMap myOutput = getTotalPhysicalEntities();
//        myOutput.put("totalNodes", getTotalNodes());
//        myOutput.put("totalRelations", getTotalRelationships());
//        myOutput.put("totalXrefNodes", getTotalXrefNodes());
//        myOutput.put("totalXrefRelations", getTotalXrefRelationships());
//        myOutput.put("totalOntologyNodes", getTotalOntologyNodes());
//        myOutput.put("totalOntologyRelations", getTotalOntologyRelationships());
//        myOutput.put("totalCelLocationNodes", getTotalCelLocationNodes());
//        myOutput.put("totalCelLocationRelations", getTotalCelLocationRelationships());
//        int time = (int) (System.currentTimeMillis() - startTime);
//
//        return new QueryResult<>("stats", time, 1, 1, null, null, Arrays.asList(myOutput));
//    }
//
//    @Override
//    public QueryResult betweenness(Query query) {
//        String id = query.getString("id");
//
//        String nodeLabel = query.getString("nodeLabel", NodeTypes.PHYSICAL_ENTITY.toString());
//        nodeLabel = nodeLabel.replace(",", "|");
//
//        String relationshipLabel = query.getString("relationshipLabel", "REACTANT");
//        relationshipLabel = relationshipLabel.replace(",", "|");
//
//        StringBuilder cypherQuery = new StringBuilder();
//        cypherQuery.append("MATCH p = allShortestPaths((source:"
//                + nodeLabel + ")-[r:" + relationshipLabel + "*]->(destination:" + nodeLabel + "))");
//        cypherQuery.append(" WHERE source <> destination AND LENGTH(NODES(p)) > 1");
//        cypherQuery.append(" WITH EXTRACT(n IN NODES(p)| n.name) AS nodes");
//        cypherQuery.append(" RETURN HEAD(nodes) AS source,");
//        cypherQuery.append(" HEAD(TAIL(TAIL(nodes))) AS destination,");
//        cypherQuery.append(" COLLECT(nodes) AS paths");
//        /*
//        Result execute = this.database.execute(cypherQuery.toString());
//        while (execute.hasNext()) {
//            Map<String, Object> next = execute.next();
//            System.out.println(next.toString());
//        }
//        */
//
//        return null;
//    }
//
//    @Override
//    public QueryResult clusteringCoefficient(Query query) {
//        // The clustering coefficient of a node is defined as the probability that two randomly
//        // selected neighbors are connected to each other. With the number of neighbors as n and
//        // the number of mutual connections between the neighbors r the calculation is:
//        // clusteringCoefficient = r/NumberOfPossibleConnectionsBetweenTwoNeighbors. Where:
//        // NumberOfPossibleConnectionsBetweenTwoNeighbors: n!/(2!(n-2)!).
//
//        Session session = this.driver.session();
//        long startTime = System.currentTimeMillis();
//
//        String ids = query.getString("id");
//        ids = ids.replace(",", "\",\"");
//        ids = ids.replace("|", "\",\"");
//
//        StringBuilder cypherQuery = new StringBuilder();
//        cypherQuery.append("UNWIND[\"" + ids + "\"] AS i");
//        cypherQuery.append(" MATCH (x:Xref { id: i })--(a)--(:Interaction)--(b)");
//        cypherQuery.append(" WITH a, count(DISTINCT b) AS n");
//        cypherQuery.append(" MATCH (a)--(:Interaction)--(:PhysicalEntity)"
//                + "--(:Interaction)-[r]-(:PhysicalEntity)--(:Interaction)--(a)");
//        cypherQuery.append(" MATCH (a)-[:CELLULAR_LOCATION]-(c:CellularLocation)");
//        cypherQuery.append(" RETURN a.name, c.id, n, count(DISTINCT r) AS r");
//
//        System.out.println(cypherQuery.toString());
//        StatementResult execute = session.run(cypherQuery.toString());
//
//        StringBuilder sb = new StringBuilder();
//        if (execute.hasNext()) {
//            sb.append("#ID\tLOCATION\tCLUSTERING_COEFFICIENT\n");
//            while (execute.hasNext()) {
//                Record result = execute.next();
//                Integer r = (int) result.get("r").asLong();
//                Integer n = (int) result.get("n").asLong();
//
//                sb.append("\"" + result.get("a.name").toString() + "\"\t"
//                        + "\"" + result.get("c.id").toString() + "\"\t");
//
//                // Computed value must fit into a double. The largest n for which n! < Double.MAX_VALUE is 170.
//                if (n > 170) {
//                    sb.append("\"NA\"\n");
//                } else if (n > 1) {
//                    double possibleConnexions = CombinatoricsUtils.factorialDouble(n)
//                            / (CombinatoricsUtils.factorialDouble(2) * (CombinatoricsUtils.factorialDouble(n - 2)));
//                    DecimalFormat df = new DecimalFormat("###.##");
//                    sb.append("\"" + df.format(r / possibleConnexions) + "\"\n");
//                } else {
//                    sb.append("\"0.00\"\n");
//                }
//            }
//        }
//
//        int time = (int) (System.currentTimeMillis() - startTime);
//
//        session.close();
//        return new QueryResult<>("clustCoeff", time, 1, 1, null, null, Arrays.asList(sb.toString()));
//    }
//
////    public boolean isClosed() {
////        return !this.session.isOpen();
////    }
//
//    public void close() {
////        this.session.close();
//        this.driver.close();
//    }
//
//    private List<Gene> getGenes(Variant variant) {
//        Set<Gene> genes  = new HashSet<>();
//        if (variant != null && variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
//            for (ConsequenceType cs: variant.getAnnotation().getConsequenceTypes()) {
//                if (cs.getGeneName() != null) {
//                    Gene gene = new Gene();
//                    gene.setName(cs.getGeneName());
//                    // TODO: annotate gene using cellbase?
//                    genes.add(gene);
//                }
//            }
//        }
//
//        List<Gene> out = new ArrayList<>();
//        Iterator<Gene> iterator = genes.iterator();
//        while (iterator.hasNext()) {
//            out.add(iterator.next());
//        }
//        return out;
//        //return new ArrayList<>(Arrays.asList((Gene[]) genes.toArray()));
//    }
//
//    private List<String> getProteinNames(Variant variant) {
//        Set<String> proteinNames  = new HashSet<>();
//        if (variant != null && variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
//            for (ConsequenceType cs: variant.getAnnotation().getConsequenceTypes()) {
//                if (cs.getProteinVariantAnnotation() != null) {
//                    if (cs.getProteinVariantAnnotation().getUniprotName() != null) {
//                        proteinNames.add(cs.getProteinVariantAnnotation().getUniprotName());
//                    }
//                    if (cs.getProteinVariantAnnotation().getUniprotAccession() != null) {
//                        proteinNames.add(cs.getProteinVariantAnnotation().getUniprotAccession());
//                    }
//                }
//            }
//        }
//        return Arrays.asList((String[]) proteinNames.toArray());
//    }
//
//    private ObjectMap parseVariant(Variant variant) {
//        ObjectMap myProperties = new ObjectMap();
//        if (variant.getId() != null) {
//            myProperties.put("id", variant.getId());
//        }
//        if (variant.getStart() != null) {
//            myProperties.put("start", variant.getStart());
//        }
//        if (variant.getEnd() != null) {
//            myProperties.put("end", variant.getEnd());
//        }
//        if (variant.getStrand() != null) {
//            myProperties.put("strand", variant.getStrand());
//        }
//        if (variant.getChromosome() != null) {
//            myProperties.put("chromosome", variant.getChromosome());
//        }
//        if (variant.getType() != null) {
//            myProperties.put("type", variant.getType().name());
//        }
//        if (variant.getAlternate() != null) {
//            myProperties.put("alternate", variant.getAlternate());
//        }
//        if (variant.getReference() != null) {
//            myProperties.put("reference", variant.getReference());
//        }
//        if (variant.getLengthReference() != null) {
//            myProperties.put("referenceLength", variant.getLengthReference());
//        }
//        if (variant.getLengthAlternate() != null) {
//            myProperties.put("alternateLength", variant.getLengthAlternate());
//        }
//        if (variant.getLength() != null) {
//            myProperties.put("length", variant.getLength());
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parseStudyEntry(StudyEntry studyEntry) {
//        ObjectMap myProperties = new ObjectMap();
//        if (studyEntry.getStudyId() != null) {
//            myProperties.put("id", studyEntry.getStudyId());
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parseFileEntry(FileEntry fileEntry) {
//        ObjectMap myProperties = new ObjectMap();
//        if (fileEntry.getFileId() != null) {
//            myProperties.put("id", fileEntry.getFileId());
//        }
//        if (fileEntry.getCall() != null) {
//            myProperties.put("call", fileEntry.getCall());
//        }
//        if (fileEntry.getAttributes() != null && MapUtils.isNotEmpty(fileEntry.getAttributes())) {
//            for (String key: fileEntry.getAttributes().keySet()) {
//                myProperties.put(key, fileEntry.getAttributes().get(key));
//            }
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parseGene(Gene gene) {
//        ObjectMap myProperties = new ObjectMap();
//        if (gene.getId() != null) {
//            myProperties.put("id", gene.getId());
//        }
//        if (gene.getName() != null) {
//            myProperties.put("name", gene.getName());
//        }
//        if (gene.getDescription() != null) {
//            myProperties.put("description", gene.getDescription());
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parsePopulationFrequency(PopulationFrequency popFreq) {
//        ObjectMap myProperties = new ObjectMap();
//        if (popFreq.getPopulation() != null) {
//            myProperties.put("population", popFreq.getPopulation());
//        }
//        if (popFreq.getAltAlleleFreq() != null) {
//            myProperties.put("altAlleleFreq", popFreq.getAltAlleleFreq());
//        }
//        if (popFreq.getRefAlleleFreq() != null) {
//            myProperties.put("refAlleleFreq", popFreq.getRefAlleleFreq());
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parseSequenceOntology(SequenceOntologyTerm so) {
//        ObjectMap myProperties = new ObjectMap();
//        if (so.getAccession() != null) {
//            myProperties.put("accession", so.getAccession());
//        }
//        if (so.getName() != null) {
//            myProperties.put("name", so.getName());
//        }
//        return myProperties;
//    }
//
//    private ObjectMap parseConsequenceType(ConsequenceType cs) {
//        ObjectMap myProperties = new ObjectMap();
//        if (cs.getEnsemblTranscriptId() != null) {
//            myProperties.put("name", cs.getEnsemblTranscriptId());
//        }
//        if (cs.getBiotype() != null) {
//            myProperties.put("biotype", cs.getBiotype());
//        }
//        if (cs.getCdnaPosition() != null) {
//            myProperties.put("cdnaPosition", cs.getCdnaPosition());
//        }
//        if (cs.getCdsPosition() != null) {
//            myProperties.put("cdsPosition", cs.getCdsPosition());
//        }
//        if (cs.getCodon() != null) {
//            myProperties.put("codon", cs.getCodon());
//        }
//        if (cs.getStrand() != null) {
//            myProperties.put("strand", cs.getStrand());
//        }
//
//        if (ListUtils.isNotEmpty(cs.getSequenceOntologyTerms())) {
//            List<ObjectMap> soProperties = new ArrayList<>();
//            for (SequenceOntologyTerm so: cs.getSequenceOntologyTerms()) {
//                ObjectMap map = new ObjectMap();
//                map.put("accession", so.getAccession());
//                map.put("name", so.getName());
//                soProperties.add(map);
//            }
//            myProperties.put("so", soProperties);
//        }
//
//        List<String> genes = new ArrayList();
//        if (cs.getEnsemblGeneId() != null) {
//            genes.add(cs.getEnsemblGeneId());
//        }
//        if (cs.getGeneName() != null) {
//            genes.add(cs.getGeneName());
//        }
//        if (ListUtils.isNotEmpty(genes)) {
//            myProperties.put("genes", genes);
//        }
//        return myProperties;
//    }
//
//    public StatementResult getProteinXrefs() {
//        Session session = this.driver.session();
//        StatementResult statementResult = session.run("MATCH (n:"
//                + PhysicalEntity.Type.PROTEIN + ")-[r:" + RelTypes.XREF + "]->(m:"
//                + NodeTypes.XREF + ") RETURN n.id, n.name, collect(m.id) as xrefs");
//        session.close();
//        return statementResult;
//    }
//
//    public StatementResult getXrefs(String nodeLabel) {
//        Session session = this.driver.session();
//        StatementResult statementResult = session.run("MATCH (n:"
//                + nodeLabel + ")-[r:" + RelTypes.XREF + "]->(m:"
//                + NodeTypes.XREF + ") RETURN n.id, n.name, collect(m.id) as xref_ids, collect(m.source) as xref_sources");
//        session.close();
//        return statementResult;
//    }
}
