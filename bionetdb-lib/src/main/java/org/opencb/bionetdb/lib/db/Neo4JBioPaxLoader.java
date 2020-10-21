package org.opencb.bionetdb.lib.db;

import org.apache.commons.lang3.StringUtils;
import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.Model;
import org.biopax.paxtools.model.level3.*;
import org.biopax.paxtools.model.level3.Process;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.opencb.commons.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Neo4JBioPaxLoader {

    private static final String REACTOME_FEAT = "reactome.";
    private static final int TRANSACTION_BATCH_SIZE = 1000;

    private Neo4JNetworkDBAdaptor networkDBAdaptor;
    private String source;
    private Map<String, Set<String>> filters;

    private Map<String, Long> rdfToUidMap;
    private Map<Long, Node.Type> uidToTypeMap;

    private List<Node> nodes;
    private List<Relation> relations;

    private long nodeLoadingTime = 0;
    private long relationLoadingTime = 0;

    private long uidCounter;

    protected static Logger logger;

    public enum FilterField {
        XREF_DBNAME ("XREF_DBNAME");

        private final String filterField;

        FilterField(String filterField) {
            this.filterField = filterField;
        }
    }

    public Neo4JBioPaxLoader(Neo4JNetworkDBAdaptor networkDBAdaptor) {
        this(networkDBAdaptor, null);
    }

    public Neo4JBioPaxLoader(Neo4JNetworkDBAdaptor networkDBAdaptor, Map<String, Set<String>> filters) {
        this.networkDBAdaptor = networkDBAdaptor;
        this.filters = filters;

        this.rdfToUidMap = new HashMap<>();
        this.uidToTypeMap = new HashMap<>();

        this.nodes = new ArrayList<>();
        this.relations = new ArrayList<>();

        this.nodeLoadingTime = 0;
        this.relationLoadingTime = 0;

        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    public void loadBioPaxFile(Path path) throws IOException {
        // First, initialize uidCounter
        uidCounter = networkDBAdaptor.getUidCounter();

        // Reading GZip input stream
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
        } else {
            inputStream = Files.newInputStream(path);
        }

        this.source = path.toFile().getName();

        // Retrieving model from BioPAX file
        BioPAXIOHandler handler = new SimpleIOHandler();
        Model model = handler.convertFromOWL(inputStream);

        // Retrieving BioPAX element
        Set<BioPAXElement> bioPAXElements = model.getObjects();

        Session session = networkDBAdaptor.getDriver().session();
        Transaction tx = null;

        Iterator<BioPAXElement> iterator = bioPAXElements.iterator();
        long numItems = bioPAXElements.size();
        long numProcessedItems = 0;

        long numNodes = 0;
        long numRelations = 0;

        long startTime = System.currentTimeMillis();

        // First loop to create all physical entity nodes
        Node node;
        while (iterator.hasNext()) {
            BioPAXElement bioPAXElement = iterator.next();
            //System.out.println(uidCounter);
            node = null;
            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                // Physical Entities
                case "PhysicalEntity": {
                    node = loadUndefinedEntity(bioPAXElement, tx);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement, tx);
                    break;
                }
                case "Dna": {
                    node = loadDna(bioPAXElement, tx);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement, tx);
                    break;
                }
                case "Rna": {
                    node = loadRna(bioPAXElement, tx);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement, tx);
                    break;
                }
                case "Protein": {
                    node = loadProtein(bioPAXElement, tx);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement, tx);
                    break;
                }
                case "Complex": {
                    node = loadComplex(bioPAXElement, tx);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement, tx);
                    break;
                }
                case "SmallMolecule": {
                    node = loadSmallMolecule(bioPAXElement, tx);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement, tx);
                    break;
                }

                // Pathways
                case "Pathway": {
                    node = loadPathway(bioPAXElement, tx);
                    updateAuxMaps(node);
                    break;
                }

                // Interactions
                case "BiochemicalReaction":
                case "TemplateReaction":
                case "Degradation":
                case "ComplexAssembly":
                case "MolecularInteraction":
                case "Transport":
                case "TransportWithBiochemicalReaction": {
                    node = loadReaction(bioPAXElement, tx);
                    updateAuxMaps(node);
                    break;
                }
                case "Catalysis": {
                    node = loadCatalysis(bioPAXElement, tx);
                    updateAuxMaps(node);
                    break;
                }
                case "Control":
                case "Modulation":
                case "TemplateReactionRegulation": {
                    node = loadRegulation(bioPAXElement, tx);
                    updateAuxMaps(node);
                    break;
                }
                default:
                    break;
            }
            if (++numProcessedItems % 10000 == 0) {
                logger.info("1: " + Math.round(100. * numProcessedItems / numItems) + "%");
            }
            if (node != null) {
                nodes.add(node);
                if (nodes.size() >= TRANSACTION_BATCH_SIZE) {
                    numNodes += nodes.size();
                    loadNodes(nodes);
                    nodes.clear();
                }
            }
            if (relations.size() > TRANSACTION_BATCH_SIZE) {
                numRelations += relations.size();
                loadRelations(relations);
                relations.clear();
            }
        }
        if (nodes.size() > 0) {
            numNodes += nodes.size();
            loadNodes(nodes);
        }

        if (relations.size() > TRANSACTION_BATCH_SIZE) {
            numRelations += relations.size();
            loadRelations(relations);
            relations.clear();
        }

        // Second loop to create relationships between physical entity nodes
        numProcessedItems = 0;
        iterator = bioPAXElements.iterator();
        while (iterator.hasNext()) {
            BioPAXElement bioPAXElement = iterator.next();
            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                case "Complex": {
                    updateComplex(bioPAXElement, tx);
                    break;
                }

                // Pathways
                case "Pathway": {
                    updatePathway(bioPAXElement, tx);
                    break;
                }

                // Interactions
                case "BiochemicalReaction":
                case "TemplateReaction":
                case "Degradation":
                case "ComplexAssembly":
                case "MolecularInteraction":
                case "Transport":
                case "TransportWithBiochemicalReaction":
                    updateReaction(bioPAXElement, tx);
                    break;
                case "Catalysis":
                    updateCatalysis(bioPAXElement, tx);
                    break;
                case "Control":
                case "Modulation":
                case "TemplateReactionRegulation":
                    updateRegulation(bioPAXElement, tx);
                    break;
                default:
                    break;
            }
            if (++numProcessedItems % 10000 == 0) {
                logger.info("2: " + Math.round(100. * numProcessedItems / numItems) + "%");
            }
            // Check batch size
            if (relations.size() > TRANSACTION_BATCH_SIZE) {
                numRelations += relations.size();
                loadRelations(relations);
                relations.clear();
            }
        }

        if (relations.size() > TRANSACTION_BATCH_SIZE) {
            numRelations += relations.size();
            loadRelations(relations);
        }

        inputStream.close();

        logger.info("Loading {} containing {} BioPax elements in {} s", path, numItems, (System.currentTimeMillis() - startTime) / 1000);
        logger.info("Loading {} nodes and at {} node/s", numNodes, Math.round(1000. * numNodes / nodeLoadingTime));
        logger.info("Loading {} relations and at {} relation/s", numRelations, Math.round(1000. * numRelations / relationLoadingTime));
//        logger.info("- First loop in {} s", (startTime2 - startTime1) / 1000);
//        logger.info("- Second loop in {} s", (System.currentTimeMillis() - startTime2) / 1000);

        // And finally, update uidCounter into the database (using the configuration node)
        networkDBAdaptor.setUidCounter(uidCounter);
    }

    void loadNodes(List<Node> nodes) {
        long startTime = System.currentTimeMillis();

        Map<String, List<Map<String, Object>>> mapsByType = new HashMap<>();
        for (Node node: nodes) {
            Map<String, Object> n = new HashMap<>();
            n.put("uid", node.getUid());
            if (StringUtils.isNotEmpty(node.getId())) {
                n.put("id", cleanValue(node.getId()));
            }
            if (StringUtils.isNotEmpty(node.getName())) {
                n.put("name", cleanValue(node.getName()));
            }
            if (StringUtils.isNotEmpty(node.getSource())) {
                n.put("source", node.getSource());
            }
            if (node.getAttributes().containsKey("uidCounter")) {
                n.put(networkDBAdaptor.PREFIX_ATTRIBUTES + "uidCounter", node.getAttributes().get("uidCounter"));
            }
            for (String key: node.getAttributes().keySet()) {
                if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                    n.put(networkDBAdaptor.PREFIX_ATTRIBUTES + key, node.getAttributes().get(key));
                } else {
                    n.put(networkDBAdaptor.PREFIX_ATTRIBUTES + key, cleanValue(node.getAttributes().getString(key)));
                }
            }

            String type = node.getType().name();
            if (!mapsByType.containsKey(type)) {
                mapsByType.put(type, new ArrayList<>());
            }
            mapsByType.get(type).add(n);
        }


        Session session = networkDBAdaptor.getDriver().session();
        try (Transaction tx = session.beginTransaction()) {
            for (String key: mapsByType.keySet()) {
                Map<String, Object> params = new HashMap<>();
                params.put("props", mapsByType.get(key));
                String cypher = "UNWIND $props AS properties CREATE (n:" + key + ") SET n = properties RETURN ID(n)";
                StatementResult ret = tx.run(cypher, params);
                while (ret.hasNext()) {
                    Record record = ret.next();
                    long uid = record.get(0).asLong();
                }
                //System.out.println("in size = " + mapsByType.get(key).size() + ", out size = " + ret.list().size());
            }
            tx.success();
            tx.close();
        }
        nodeLoadingTime += System.currentTimeMillis() - startTime;
        logger.info("{}-node batch in {} s at {} node/s", nodes.size(), (System.currentTimeMillis() - startTime) / 1000.0,
                Math.round(1000.0 * nodes.size() / ((System.currentTimeMillis() - startTime))));
    }

    void loadRelations(List<Relation> relations) {
        long startTime = System.currentTimeMillis();

        Map<String, List<Map<String, Object>>> mapsByType = new HashMap<>();

        for (Relation relation: relations) {
            Map<String, Object> r = new HashMap<>();
            if (StringUtils.isNotEmpty(relation.getName())) {
                r.put("name", cleanValue(relation.getName()));
            }
            if (StringUtils.isNotEmpty(relation.getSource())) {
                r.put("source", relation.getSource());
            }
            for (String key : relation.getAttributes().keySet()) {
                if (StringUtils.isNumeric(relation.getAttributes().getString(key))) {
                    r.put(networkDBAdaptor.PREFIX_ATTRIBUTES + key, relation.getAttributes().get(key));
                } else {
                    r.put(networkDBAdaptor.PREFIX_ATTRIBUTES + key, cleanValue(relation.getAttributes().getString(key)));
                }
            }

            String key = relation.getOrigType().name() + ":" + relation.getDestType().name() + ":" + relation.getType().name();
            if (!mapsByType.containsKey(key)) {
                mapsByType.put(key, new ArrayList<>());
            }
            Map<String, Object> map = new HashMap<>();
            map.put("origUid", relation.getOrigUid());
            map.put("destUid", relation.getDestUid());
            map.put("properties", r);
            mapsByType.get(key).add(map);
        }

        Session session = networkDBAdaptor.getDriver().session();
        try (Transaction tx = session.beginTransaction()) {
            for (String key: mapsByType.keySet()) {
                Map<String, Object> params = new HashMap<>();
                String[] split = key.split(":");
                params.put("maps", mapsByType.get(key));
                StringBuilder cypher = new StringBuilder("UNWIND $maps AS map MATCH (o:")
                        .append(split[0]).append("{uid:map.origUid}) MATCH (d:")
                        .append(split[1]).append("{uid:map.destUid}) USING INDEX d:")
                        .append(split[1]).append("(uid) MERGE (o)-[r:")
                        .append(split[2]).append("]->(d) SET r = map.properties");
                tx.run(cypher.toString(), params);
            }
            tx.success();
            tx.close();
        }
        relationLoadingTime += System.currentTimeMillis() - startTime;
        logger.info("{}-relation batch in {} s at {} relation/s", relations.size(), (System.currentTimeMillis() - startTime) / 1000.0,
                Math.round(1000.0 * relations.size() / ((System.currentTimeMillis() - startTime))));
    }

    public StatementResult addNode(Node node, Transaction tx) {
        // Gather properties of the node to create a cypher string with them
        List<String> props = new ArrayList<>();
        props.add("n.uid=" + node.getUid());
        if (StringUtils.isNotEmpty(node.getId())) {
            props.add("n.id=\"" + cleanValue(node.getId()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getName())) {
            props.add("n.name=\"" + cleanValue(node.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getSource())) {
            props.add("n.source=\"" + node.getSource() + "\"");
        }
        if (node.getAttributes().containsKey("uidCounter")) {
            props.add("n." + networkDBAdaptor.PREFIX_ATTRIBUTES + "uidCounter=" + node.getAttributes().get("uidCounter"));
        }
        for (String key: node.getAttributes().keySet()) {
            if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                props.add("n." + networkDBAdaptor.PREFIX_ATTRIBUTES + key + "=" + node.getAttributes().getString(key));
            } else {
                props.add("n." + networkDBAdaptor.PREFIX_ATTRIBUTES + key + "=\"" + cleanValue(node.getAttributes().getString(key)) + "\"");
            }
        }
        //String propsJoined = "{" + String.join(",", props) + "}";

        // Create the desired node
        StringBuilder cypher = new StringBuilder("CREATE (n");
        if (ListUtils.isNotEmpty(node.getTags())) {
            cypher.append(":").append(StringUtils.join(node.getTags(), ":"));
        }
        cypher.append(")");
        if (ListUtils.isNotEmpty(props)) {
            cypher.append(" SET ").append(StringUtils.join(props, ","));
        }
        //cypher.append(" RETURN ID(n) AS UID");
        StatementResult ret = tx.run(cypher.toString());
        //node.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    private String cleanValue(String value) {
        return value.replace("\"", ",").replace("\\", "|");
    }

    //-------------------------------------------------------------------------
    // NODE CREATION
    //-------------------------------------------------------------------------

    private Node loadUndefinedEntity(BioPAXElement bioPAXElement, Transaction tx) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.UNDEFINED, source);

        // Common properties
        setPhysicalEntityCommonProperties(physicalEntityBP, node);

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadDna(BioPAXElement bioPAXElement, Transaction tx) {
        Dna dnaBP = (Dna) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.DNA, source);

        // Common properties
        setPhysicalEntityCommonProperties(dnaBP, node);

        // Dna properties
        if (dnaBP.getEntityReference() != null) {
            EntityReference entityReference = dnaBP.getEntityReference();
//
//            // altIds
//            for (String name : entityReference.getName()) {
//                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
//                xref.setSource(REACTOME_FEAT + "biopax");
//                xref.setId(name);
////                dna.setXref(xref);
//            }
//
            // description
            addSetAttributes(entityReference.getComment(), "description", node);

//            // xref
//            Set<Xref> xrefs = entityReference.getXref();
//            for (Xref xref : xrefs) {
//                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
//                x.setSource(xref.getDb());
//                x.setSourceVersion(xref.getDbVersion());
//                x.setId(xref.getId());
//                x.setIdVersion(xref.getIdVersion());
////                dna.setXref(x);
//            }
        }

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadRna(BioPAXElement bioPAXElement, Transaction tx) {
        Rna rnaBP = (Rna) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.RNA, source);

        // Common properties
        setPhysicalEntityCommonProperties(rnaBP, node);

        // Rna properties
        if (rnaBP.getEntityReference() != null) {
            EntityReference entityReference = rnaBP.getEntityReference();

//            // altIds
//            for (String name : entityReference.getName()) {
//                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
//                xref.setSource(REACTOME_FEAT + "biopax");
//                xref.setId(name);
////                rna.setXref(xref);
//            }

            // description
            addSetAttributes(entityReference.getComment(), "description", node);

//            // xref
//            Set<Xref> xrefs = entityReference.getXref();
//            for (Xref xref : xrefs) {
//                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
//                x.setSource(xref.getDb());
//                x.setSourceVersion(xref.getDbVersion());
//                x.setId(xref.getId());
//                x.setIdVersion(xref.getIdVersion());
////                rna.setXref(x);
//            }
        }

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadProtein(BioPAXElement bioPAXElement, Transaction tx) {
        Protein proteinBP = (Protein) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.PROTEIN, source);

        // Common properties
        setPhysicalEntityCommonProperties(proteinBP, node);

        // Protein properties
        if (proteinBP.getEntityReference() != null) {
            EntityReference entityReference = proteinBP.getEntityReference();
//
//            // altIds
//            for (String name : entityReference.getName()) {
//                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
//                xref.setSource(REACTOME_FEAT + "biopax");
//                xref.setId(name);
//                protein.setXref(xref);
//            }
//
            // description
            addSetAttributes(entityReference.getComment(), "description", node);
//
//            // xref
//            Set<Xref> xrefs = entityReference.getXref();
//            for (Xref xref : xrefs) {
//                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
//                x.setSource(xref.getDb());
//                x.setSourceVersion(xref.getDbVersion());
//                x.setId(xref.getId());
//                x.setIdVersion(xref.getIdVersion());
//                protein.setXref(x);
//            }
        }

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadSmallMolecule(BioPAXElement bioPAXElement, Transaction tx) {
        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.SMALL_MOLECULE, source);

        // Common properties
        setPhysicalEntityCommonProperties(smallMoleculeBP, node);

        // SmallMolecule properties
        if (smallMoleculeBP.getEntityReference() != null) {
            EntityReference entityReference = smallMoleculeBP.getEntityReference();
//
//            // altIds
//            for (String name : entityReference.getName()) {
//                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
//                xref.setSource(REACTOME_FEAT + "biopax");
//                xref.setId(name);
//                smallMolecule.setXref(xref);
//            }

            // description
            addSetAttributes(entityReference.getComment(), "description", node);

//            // xref
//            Set<Xref> xrefs = entityReference.getXref();
//            for (Xref xref : xrefs) {
//                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
//                x.setSource(xref.getDb());
//                x.setSourceVersion(xref.getDbVersion());
//                x.setId(xref.getId());
//                x.setIdVersion(xref.getIdVersion());
//                smallMolecule.setXref(x);
//            }
        }

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadComplex(BioPAXElement bioPAXElement, Transaction tx) {
        Complex complexBP = (Complex) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.COMPLEX, source);

        // Common properties
        setPhysicalEntityCommonProperties(complexBP, node);

        // Complex properties

        // Component node and stoichiometry are added later

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private void setPhysicalEntityCommonProperties(PhysicalEntity physicalEntityBP, Node physicalEntityNode) {
        // = SPECIFIC PROPERTIES =
        // id
        physicalEntityNode.setId(physicalEntityBP.getRDFId().split("#")[1]);

        // name
        if (physicalEntityBP.getDisplayName() != null) {
            physicalEntityNode.setName(physicalEntityBP.getDisplayName());
//            physicalEntity.setXref(new org.opencb.bionetdb.core.models.Xref(REACTOME_FEAT + "biopax",
//                    "", physicalEntityBP.getDisplayName(), ""));
        }

//        // altNames
//        for (String name : physicalEntityBP.getName()) {
//            physicalEntity.setXref(new org.opencb.bionetdb.core.models.Xref(REACTOME_FEAT + "biopax", "", name, ""));
//        }

//        // cellularLocation
//        CellularLocation cellularLocation = new CellularLocation();
//        for (String name : physicalEntityBP.getCellularLocation().getTerm()) {
//            cellularLocation.setName(name);
//        }
//        for (Xref cellLocXref : physicalEntityBP.getCellularLocation().getXref()) {
//            Ontology ontology= new Ontology();
//            if (cellLocXref.getDb().toLowerCase().equals("gene ontology")) {
//                ontology.setSource("go");
//                ontology.setId(cellLocXref.getId().split(":")[1]);
//            } else {
//                ontology.setSource(cellLocXref.getDb());
//                ontology.setId(cellLocXref.getId());
//            }
//            ontology.setSourceVersion(cellLocXref.getDbVersion());
//            ontology.setIdVersion(cellLocXref.getIdVersion());
//            cellularLocation.setOntology(ontology);
//        }
//        physicalEntity.getCellularLocation().add(cellularLocation);

//        // source
//        List<String> sources = new ArrayList<>();
//        for (Provenance provenance : physicalEntityBP.getDataSource()) {
//            sources.addAll(provenance.getName());
//        }
//        physicalEntity.setSource(sources);

        // TODO [WARNING]: MemberPhysicalEntity and MemberPhysicalEntityOf
        // Please avoid using this property in your BioPAX L3 models unless absolutely sure/required,
        // for there is an alternative way (using PhysicalEntity/entityReference/memberEntityReference),
        // and this will probably be deprecated in the future BioPAX releases.
        // http://www.biopax.org/m2site/paxtools-4.2.0/apidocs/org/biopax/paxtools/impl/level3/PhysicalEntityImpl.html

//        // members
//        for (PhysicalEntity pe : physicalEntityBP.getMemberPhysicalEntity()) {
//            physicalEntity.getMembers().add(pe.getRDFId().split("#")[1]);
//        }

//        // memberOfSet
//        for (PhysicalEntity peOf : physicalEntityBP.getMemberPhysicalEntityOf()) {
//            physicalEntity.getMemberOfSet().add(peOf.getRDFId().split("#")[1]);
//        }

//        // componentOfComplex
//        for (Complex complex : physicalEntityBP.getComponentOf()) {
//            physicalEntity.getComponentOfComplex().add(complex.getRDFId().split("#")[1]);
//        }

        // participantOfInteraction
//        for (Interaction interaction : physicalEntityBP.getParticipantOf()) {
//            physicalEntity.getParticipantOfInteraction().add(interaction.getRDFId().split("#")[1]);
//        }

        // xrefs
//        Set<Xref> xrefs = physicalEntityBP.getXref();
//        for (Xref xref : xrefs) {
//            if (xref.getDb() != null) {
//                String source = xref.getDb().toLowerCase();
//                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
//                    physicalEntity.setOntology(new Ontology(xref.getDb(), xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
//                } else if (source.equals("pubmed")) {
//                    physicalEntity.setPublication(new Publication(xref.getDb(), xref.getId()));
//                } else {
//                    physicalEntity.setXref(new org.opencb.bionetdb.core.models.Xref(xref.getDb(),
//                            xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
//                }
//            }
//        }

        // publications
//        for (Evidence evidence : physicalEntityBP.getEvidence()) {
//            for (Xref xref : evidence.getXref()) {
//                PublicationXref pubXref = (PublicationXref) xref;
//                Publication publication = new Publication();
//                publication.setSource(pubXref.getDb());
//                publication.setId(pubXref.getId());
//                publication.setTitle(pubXref.getTitle());
//                publication.setYear(pubXref.getYear());
//                for (String author : pubXref.getAuthor()) {
//                    publication.setAuthor(author);
//                }
//                for (String source : pubXref.getSource()) {
//                    publication.setJournal(source);
//                }
//                physicalEntity.setPublication(publication);
//            }
//        }

        // = NONSPECIFIC PROPERTIES =
        // comment
        addSetAttributes(physicalEntityBP.getComment(), "comment", physicalEntityNode);

        // availability
        addSetAttributes(physicalEntityBP.getAvailability(), "availability", physicalEntityNode);
        if (physicalEntityBP.getAvailability() != null) {
            String availability = StringUtils.join(physicalEntityBP.getAvailability(), ";");
            if (StringUtils.isNotEmpty(availability)) {
                physicalEntityNode.addAttribute("availability", availability);
            }
        }

        // annotations
        addMapAttributes(physicalEntityBP.getAnnotations(), "annot", physicalEntityNode);

//        // features
//        List<Map<String, Object>> features = new ArrayList<>();
//        Set<EntityFeature> entityFeatures = physicalEntityBP.getFeature();
//        for (EntityFeature entityFeature : entityFeatures) {
//            Map<String, Object> feature = new HashMap<>();
//            String featureName = entityFeature.getModelInterface().getSimpleName();
//            feature.put("type", featureName);
//            feature.put("name", entityFeature.toString());
//            features.add(feature);
//        }
//        physicalEntity.setFeatures(features);
    }

    private Node loadPathway(BioPAXElement bioPAXElement, Transaction tx) {
        Pathway pathwayBP = (Pathway) bioPAXElement;
        Node node = new Node(++uidCounter, getBioPaxId(pathwayBP.getRDFId()), pathwayBP.getDisplayName(), Node.Type.PATHWAY, source);

        // Common properties
//        setPhysicalEntityCommonProperties(complexBP, node);

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadReaction(BioPAXElement bioPAXElement, Transaction tx) {
        String className = bioPAXElement.getModelInterface().getSimpleName();
        Node node = new Node(++uidCounter, null, null, Node.Type.REACTION, source);

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                // Setting up reaction type
                node.setType(Node.Type.REACTION);

                // Common Interaction properties
                setInteractionCommonProperties(templateReactBP, node);

                // TemplateReaction properties

                // Reactant nodes/relationships are added later
                // Product nodes/relationships are added later

                break;
            case "BiochemicalReaction":
            case "Degradation":
            case "ComplexAssembly":
            case "Transport":
            case "TransportWithBiochemicalReaction":
                Conversion conversionBP = (Conversion) bioPAXElement;

                // Setting up reaction type
                switch (className) {
                    case "BiochemicalReaction":
                    case "Degradation":
                        node.setType(Node.Type.REACTION);
                        break;
                    case "ComplexAssembly":
                        node.setType(Node.Type.ASSEMBLY);
                        break;
                    case "Transport":
                    case "TransportWithBiochemicalReaction":
                        node.setType(Node.Type.TRANSPORT);
                        break;
                    default:
                        break;
                }

                // Common Interaction properties
                setInteractionCommonProperties(conversionBP, node);

                // Stoichiometry coefficients are added later
                // Left and Right nodes/relationships are added later

                // Spontaneous
                if (conversionBP.getSpontaneous() != null) {
                    if (conversionBP.getSpontaneous()) {
                        node.addAttribute("spontaneous", true);
                    } else {
                        node.addAttribute("spontaneous", false);
                    }
                }

                // Adding EC number to xrefs
//                if (className.equals("BiochemicalReaction")) {
//                    BiochemicalReaction br = (BiochemicalReaction) bioPAXElement;
//                    for (String ecNumber : br.getECNumber()) {
//                        Ontology ontology = new Ontology();
//                        ontology.setSource("ec");
//                        ontology.setId(ecNumber);
//                        reaction.setOntology(ontology);
//                    }
//                }

                break;
            case "MolecularInteraction":
                MolecularInteraction molecularInteractionBP = (MolecularInteraction) bioPAXElement;

                // Setting up reaction type
                node.setType(Node.Type.ASSEMBLY);

                // Common Interaction properties
                setInteractionCommonProperties(molecularInteractionBP, node);
                break;
            default:
                break;
        }

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadCatalysis(BioPAXElement bioPAXElement, Transaction tx) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.CATALYSIS, source);

        // Common Interaction properties
        setInteractionCommonProperties(catalysisBP, node);

        // Catalysis properties

        // Controller relationships are added later
        // Controlled relationships are added later

        // ControlType
        node.addAttribute("controlType", catalysisBP.getControlType().toString());

        // Cofactor nodes/relationships are added later

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadRegulation(BioPAXElement bioPAXElement, Transaction tx) {
        Control controlBP = (Control) bioPAXElement;
        Node node = new Node(++uidCounter, null, null, Node.Type.REGULATION, source);

        // Common Interaction properties
        setInteractionCommonProperties(controlBP, node);

        // Regulation properties

        // Controller relationships are added later
        // Controlled relationships are added later

        // ControlType
        node.addAttribute("controlType", controlBP.getControlType().toString());

        //networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private void setInteractionCommonProperties(Interaction interactionBP, Node interactionNode) {
        // = SPECIFIC PROPERTIES =
        // id
        interactionNode.setId(interactionBP.getRDFId().split("#")[1]);

//        // description
//        List<String> descs = new ArrayList<>();
//        if (interactionBP.getComment() != null) {
//            for (String comment : interactionBP.getComment()) {
//                if (!comment.matches("(Authored:|Edited:|Reviewed:).+")) {
//                    descs.add(comment);
//                }
//            }
//            interactionNode.setDescription(descs);
//        }

        // name
        if (interactionBP.getDisplayName() != null) {
            interactionNode.setName(interactionBP.getDisplayName());
//            interactionNode.setXref(new org.opencb.bionetdb.core.models.Xref(REACTOME_FEAT + "biopax",
//                    "", interactionBP.getDisplayName(), ""));
        }

        // source
//        List<String> sources = new ArrayList<>();
//        for (Provenance provenance : interactionBP.getDataSource()) {
//            sources.addAll(provenance.getName());
//        }
//        interactionNode.setSource(sources);

        // participants
//        for (Entity entity : interactionBP.getParticipant()) {
//            interactionNode.getParticipants().add(entity.getRDFId().split("#")[1]);
//        }

        // controlledBy
//        for (Control control : interactionBP.getControlledOf()) {
//            interactionNode.getControlledBy().add(control.getRDFId().split("#")[1]);
//        }

        // processOfPathway
//        for (PathwayStep pathwayStep : interactionBP.getStepProcessOf()) {
//            interactionNode.getProcessOfPathway().add(pathwayStep.getPathwayOrderOf().getRDFId().split("#")[1]);
//        }

        // xref
//        for (Xref xref : interactionBP.getXref()) {
//            if (xref.getDb() != null) {
//                String source = xref.getDb().toLowerCase();
//                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
//                    interactionNode.setOntology(new Ontology(xref.getDb(), xref.getDbVersion(),
//                            xref.getId(), xref.getIdVersion()));
//                } else if (source.equals("pubmed")) {
//                    interactionNode.setPublication(new Publication(xref.getDb(), xref.getId()));
//                } else {
//                    interactionNode.setXref(new org.opencb.bionetdb.core.models.Xref(xref.getDb(),
//                            xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
//                }
//            }
//        }

        // publications
//        for (Evidence evidence : interactionBP.getEvidence()) {
//            for (Xref xref : evidence.getXref()) {
//                PublicationXref pubXref = (PublicationXref) xref;
//                Publication publication = new Publication();
//                publication.setSource(pubXref.getDb());
//                publication.setId(pubXref.getId());
//                publication.setTitle(pubXref.getTitle());
//                publication.setYear(pubXref.getYear());
//                for (String author : pubXref.getAuthor()) {
//                    publication.setAuthor(author);
//                }
//                for (String source : pubXref.getSource()) {
//                    publication.setJournal(source);
//                }
//                interactionNode.setPublication(publication);
//            }
//        }

        // = NONSPECIFIC PROPERTIES =
        // availability
        addSetAttributes(interactionBP.getAvailability(), "availability", interactionNode);

        // annotations
        addMapAttributes(interactionBP.getAnnotations(), "annot", interactionNode);

        // interactionType
        if (interactionBP.getInteractionType() != null) {
            StringBuilder types = new StringBuilder();
            Iterator<InteractionVocabulary> iterator = interactionBP.getInteractionType().iterator();
            while (iterator.hasNext()) {
                InteractionVocabulary item = iterator.next();
                if (types.length() > 0) {
                    types.append(";");
                }
                types.append(StringUtils.join(item.getTerm(), ";"));
            }
            if (types.length() > 0) {
                interactionNode.addAttribute("interactionTypes", types);
            }
        }
    }

    //-------------------------------------------------------------------------
    // RELATIONSHIP CREATION
    //-------------------------------------------------------------------------

    private void updatePhysicalEntity(BioPAXElement bioPAXElement, Transaction tx) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;

        String physicalEntityId = getBioPaxId(physicalEntityBP.getRDFId());
        Long physicalEntityUid = rdfToUidMap.get(physicalEntityId);
        Node.Type physicalEntityType = uidToTypeMap.get(physicalEntityUid);

        // = SPECIFIC PROPERTIES =

        // name
        if (physicalEntityBP.getDisplayName() != null) {
            addXref(physicalEntityBP.getDisplayName(), REACTOME_FEAT + "biopax", physicalEntityUid, physicalEntityType, tx);
        }

        // altNames
        for (String name: physicalEntityBP.getName()) {
            addXref(name, REACTOME_FEAT + "biopax", physicalEntityUid, physicalEntityType, tx);
        }

        // cellularLocation
        Node cellularLocNode = null;
        for (String name: physicalEntityBP.getCellularLocation().getTerm()) {
            cellularLocNode = new Node(uidCounter, null, name, Node.Type.CELLULAR_LOCATION, source);
            //networkDBAdaptor.mergeNode(cellularLocNode, "name", tx);
            nodes.add(cellularLocNode);


            Relation relation = new Relation(++uidCounter, null, physicalEntityUid, uidToTypeMap.get(physicalEntityUid),
                    cellularLocNode.getUid(), cellularLocNode.getType(), Relation.Type.CELLULAR_LOCATION, source);
            //networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);

            // Get the first term
            break;
        }
        if (cellularLocNode != null) {
            addSetXref(physicalEntityBP.getCellularLocation().getXref(), cellularLocNode.getUid(), cellularLocNode.getType(), tx);
        }

//        // source
//        List<String> sources = new ArrayList<>();
//        for (Provenance provenance : physicalEntityBP.getDataSource()) {
//            sources.addAll(provenance.getName());
//        }
//        physicalEntity.setSource(sources);

        // TODO [WARNING]: MemberPhysicalEntity and MemberPhysicalEntityOf
        // Please avoid using this property in your BioPAX L3 models unless absolutely sure/required,
        // for there is an alternative way (using PhysicalEntity/entityReference/memberEntityReference),
        // and this will probably be deprecated in the future BioPAX releases.
        // http://www.biopax.org/m2site/paxtools-4.2.0/apidocs/org/biopax/paxtools/impl/level3/PhysicalEntityImpl.html

        // members, memberOfSet, componentOfComplex and participantOfInteraction relationships are added later

        // xrefs
        addSetXref(physicalEntityBP.getXref(), physicalEntityUid, physicalEntityType, tx);

        // publications
//        for (Evidence evidence : physicalEntityBP.getEvidence()) {
//            for (Xref xref : evidence.getXref()) {
//                PublicationXref pubXref = (PublicationXref) xref;
//                Publication publication = new Publication();
//                publication.setSource(pubXref.getDb());
//                publication.setId(pubXref.getId());
//                publication.setTitle(pubXref.getTitle());
//                publication.setYear(pubXref.getYear());
//                for (String author : pubXref.getAuthor()) {
//                    publication.setAuthor(author);
//                }
//                for (String source : pubXref.getSource()) {
//                    publication.setJournal(source);
//                }
//                physicalEntity.setPublication(publication);
//            }
//        }

        // = NONSPECIFIC PROPERTIES =

//        // features
//        List<Map<String, Object>> features = new ArrayList<>();
//        Set<EntityFeature> entityFeatures = physicalEntityBP.getFeature();
//        for (EntityFeature entityFeature : entityFeatures) {
//            Map<String, Object> feature = new HashMap<>();
//            String featureName = entityFeature.getModelInterface().getSimpleName();
//            feature.put("type", featureName);
//            feature.put("name", entityFeature.toString());
//            features.add(feature);
//        }
//        physicalEntity.setFeatures(features);
    }

    private void updateComplex(BioPAXElement bioPAXElement, Transaction tx) {
        Complex complexBP = (Complex) bioPAXElement;

        String complexId = getBioPaxId(complexBP.getRDFId());
        Long complexUid = rdfToUidMap.get(complexId);
        Node.Type complexType = uidToTypeMap.get(complexUid);

        // Stoichiometry
        Map<Long, Float> stoichiometryMap = new HashMap<>();
        Set<Stoichiometry> stoichiometryItems = complexBP.getComponentStoichiometry();
        for (Stoichiometry stoichiometryItem: stoichiometryItems) {
            String peId = getBioPaxId(stoichiometryItem.getPhysicalEntity().getRDFId());
            Long peUid = rdfToUidMap.get(peId);
            stoichiometryMap.put(peUid, stoichiometryItem.getStoichiometricCoefficient());
        }

        // Components
        Set<PhysicalEntity> components = complexBP.getComponent();
        for (PhysicalEntity component: components) {
            Long componentUid = rdfToUidMap.get(getBioPaxId(component.getRDFId()));
            Node.Type componentType = uidToTypeMap.get(componentUid);
            Relation relation = new Relation(++uidCounter, null, componentUid, componentType, complexUid, complexType,
                    Relation.Type.COMPONENT_OF_COMPLEX, source);
            if (stoichiometryMap.containsKey(componentUid)) {
                relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(componentUid));
            }
            //networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }
    }


    private void updatePathway(BioPAXElement bioPAXElement, Transaction tx) {
        Pathway pathwayBP = (Pathway) bioPAXElement;

        String pathwayId = getBioPaxId(pathwayBP.getRDFId());
        Long pathwayUid = rdfToUidMap.get(pathwayId);
        Node.Type pathwayType = uidToTypeMap.get(pathwayUid);

        // Components
        Set<Process> components = pathwayBP.getPathwayComponent();
        for (Process component: components) {
            Long componentUid = rdfToUidMap.get(getBioPaxId(component.getRDFId()));
            Node.Type componentType = uidToTypeMap.get(componentUid);
            Relation relation = new Relation(++uidCounter, null, componentUid, componentType, pathwayUid, pathwayType,
                    Relation.Type.COMPONENT_OF_PATHWAY, source);
            //networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }

        //
        Set<PathwayStep> pathwayOrder = pathwayBP.getPathwayOrder();
        for (PathwayStep pathwayStep: pathwayOrder) {
            for (Process currentStep: pathwayStep.getStepProcess()) {
                Long currentStepUid = rdfToUidMap.get(getBioPaxId(currentStep.getRDFId()));
                Node.Type currentStepType = uidToTypeMap.get(currentStepUid);

                for (PathwayStep pathwayNextStep: pathwayStep.getNextStep()) {
                    for (Process nextStep: pathwayNextStep.getStepProcess()) {
                        if (rdfToUidMap.containsKey(getBioPaxId(nextStep.getRDFId()))) {
                            Long nextStepUid = rdfToUidMap.get(getBioPaxId(nextStep.getRDFId()));
                            Node.Type nextStepType = uidToTypeMap.get(nextStepUid);
                            try {
                                Relation relation = new Relation(++uidCounter, null, currentStepUid, currentStepType, nextStepUid,
                                        nextStepType, Relation.Type.PATHWAY_NEXT_STEP, source);
//                                networkDBAdaptor.addRelation(relation, tx);
                                relations.add(relation);
                            } catch (Exception e) {
                                logger.info("impossible create realtionship: " + e.getMessage());
                                logger.info("current step: {}, uid {}, type {}", currentStep.getRDFId(), currentStepUid, currentStepType);
                                logger.info("next step   : {}, uid {}, type {}", nextStep.getRDFId(), nextStepUid, nextStepType);
                            }
                        }
                    }
                }
            }
        }

    }

    private void updateReaction(BioPAXElement bioPAXElement, Transaction tx) {
        String className = bioPAXElement.getModelInterface().getSimpleName();

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                String templateReactId = getBioPaxId(templateReactBP.getRDFId());
                Long templateReactUid = rdfToUidMap.get(templateReactId);
                Node.Type templateReactType = uidToTypeMap.get(templateReactUid);

                // TemplateReaction properties

                // Reactants
                if (templateReactBP.getTemplate() != null) {
                    Long reactantUid = rdfToUidMap.get(getBioPaxId(templateReactBP.getTemplate().getRDFId()));
                    Node.Type reactantType = uidToTypeMap.get(reactantUid);
                    Relation relation = new Relation(++uidCounter, null, templateReactUid, templateReactType, reactantUid, reactantType,
                            Relation.Type.REACTANT, source);
//                    networkDBAdaptor.addRelation(relation, tx);
                    relations.add(relation);
                }

                // Products
                Set<PhysicalEntity> products = templateReactBP.getProduct();
                for (PhysicalEntity product: products) {
                    Long productUid = rdfToUidMap.get(getBioPaxId(product.getRDFId()));
                    Node.Type productType = uidToTypeMap.get(productUid);
                    Relation relation = new Relation(++uidCounter, null, templateReactUid, templateReactType, productUid, productType,
                            Relation.Type.PRODUCT, source);
//                    networkDBAdaptor.addRelation(relation, tx);
                    relations.add(relation);
                }
                break;
            case "BiochemicalReaction":
            case "Degradation":
            case "ComplexAssembly":
            case "Transport":
            case "TransportWithBiochemicalReaction":
                Conversion conversionBP = (Conversion) bioPAXElement;

                String conversionId = getBioPaxId(conversionBP.getRDFId());
                Long conversionUid = rdfToUidMap.get(conversionId);
                Node.Type conversionType = uidToTypeMap.get(conversionUid);

                // Left items
                List<String> leftItems = new ArrayList<>();
                Set<PhysicalEntity> lefts = conversionBP.getLeft();
                for (PhysicalEntity left: lefts) {
                    leftItems.add(getBioPaxId(left.getRDFId()));
                }

                // Right items
                List<String> rightItems = new ArrayList<>();
                Set<PhysicalEntity> rights = conversionBP.getRight();
                for (PhysicalEntity right: rights) {
                    rightItems.add(getBioPaxId(right.getRDFId()));
                }

                Relation.Type type1 = null;
                Relation.Type type2 = null;
                if (conversionBP.getConversionDirection() != null) {
                    switch (conversionBP.getConversionDirection().toString()) {
                        case "LEFT-TO-RIGHT":
                        case "LEFT_TO_RIGHT":
                            type1 = Relation.Type.REACTANT;
                            type2 = Relation.Type.PRODUCT;
                            break;
                        case "RIGHT-TO-LEFT":
                        case "RIGHT_TO_LEFT":
                            type1 = Relation.Type.PRODUCT;
                            type2 = Relation.Type.REACTANT;
                            break;
                        default:
                            break;
                    }
                } else {
                    type1 = Relation.Type.REACTANT;
                    type2 = Relation.Type.PRODUCT;
                }

                // Stoichiometry
                Map<String, Float> stoichiometryMap = new HashMap<>();
                Set<Stoichiometry> stoichiometryItems = conversionBP.getParticipantStoichiometry();
                for (Stoichiometry stoichiometryItem : stoichiometryItems) {
                    stoichiometryMap.put(stoichiometryItem.getPhysicalEntity().getRDFId(),
                            stoichiometryItem.getStoichiometricCoefficient());
                }

                if (type1 != null && type2 != null) {
                    // Reactants
                    for (String id: leftItems) {
                        Long uid = rdfToUidMap.get(id);
                        Node.Type type = uidToTypeMap.get(uid);
                        Relation relation = new Relation(++uidCounter, null, conversionUid, conversionType, uid, type, type1, source);
                        if (stoichiometryMap.containsKey(id)) {
                            relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(id));
                        }
//                        networkDBAdaptor.addRelation(relation, tx);
                        relations.add(relation);
                    }

                    // Products
                    for (String id: rightItems) {
                        Long uid = rdfToUidMap.get(id);
                        Node.Type type = uidToTypeMap.get(uid);
                        Relation relation = new Relation(++uidCounter, null, conversionUid, conversionType, uid, type, type2, source);
                        if (stoichiometryMap.containsKey(id)) {
                            relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(id));
                        }
//                        networkDBAdaptor.addRelation(relation, tx);
                        relations.add(relation);
                    }
                }

                // Adding EC number to xrefs
//                if (className.equals("BiochemicalReaction")) {
//                    BiochemicalReaction br = (BiochemicalReaction) bioPAXElement;
//                    for (String ecNumber : br.getECNumber()) {
//                        Ontology ontology = new Ontology();
//                        ontology.setSource("ec");
//                        ontology.setId(ecNumber);
//                        reaction.setOntology(ontology);
//                    }
//                }

                break;
            default:
                break;
        }
    }

    private void updateCatalysis(BioPAXElement bioPAXElement, Transaction tx) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;

        String catalysisId = getBioPaxId(catalysisBP.getRDFId());
        Long catalysisUid = rdfToUidMap.get(catalysisId);
        Node.Type catalysisType = uidToTypeMap.get(catalysisUid);

        // Controllers
        Set<Controller> controllers = catalysisBP.getController();
        for (Controller controller: controllers) {
            Long controllerUid = rdfToUidMap.get(getBioPaxId(controller.getRDFId()));
            Node.Type controllerType = uidToTypeMap.get(controllerUid);
            Relation relation = new Relation(++uidCounter, null, catalysisUid, catalysisType, controllerUid, controllerType,
                    Relation.Type.CONTROLLER, source);
//            networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }

        // Controlled
        Set<Process> controlledProcesses = catalysisBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            Long controlledUid = rdfToUidMap.get(getBioPaxId(controlledProcess.getRDFId()));
            Node.Type controlledType = uidToTypeMap.get(controlledUid);
            Relation relation = new Relation(++uidCounter, null, catalysisUid, catalysisType, controlledUid, controlledType,
                    Relation.Type.CONTROLLED, source);
//            networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }

        // Cofactor
        Set<PhysicalEntity> cofactors = catalysisBP.getCofactor();
        for (PhysicalEntity cofactor: cofactors) {
            Long cofactorUid = rdfToUidMap.get(getBioPaxId(cofactor.getRDFId()));
            Node.Type cofactorType = uidToTypeMap.get(cofactorUid);
            Relation relation = new Relation(++uidCounter, null, catalysisUid, catalysisType, cofactorUid, cofactorType,
                    Relation.Type.COFACTOR, source);
//            networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }
    }

    private void updateRegulation(BioPAXElement bioPAXElement, Transaction tx) {
        Control controlBP = (Control) bioPAXElement;

        String controlId = getBioPaxId(controlBP.getRDFId());
        Long controlUid = rdfToUidMap.get(controlId);
        Node.Type controlType = uidToTypeMap.get(controlUid);

        // Regulation properties

        // Controllers
        Set<Controller> controllers = controlBP.getController();
        for (Controller controller: controllers) {
            Long controllerUid = rdfToUidMap.get(getBioPaxId(controller.getRDFId()));
            Node.Type controllerType = uidToTypeMap.get(controllerUid);
            Relation relation = new Relation(++uidCounter, null, controlUid, controlType, controllerUid, controllerType,
                    Relation.Type.CONTROLLER, source);
//            networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }

        // Controlled
        Set<Process> controlledProcesses = controlBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            Long controlledUid = rdfToUidMap.get(getBioPaxId(controlledProcess.getRDFId()));
            Node.Type controlledType = uidToTypeMap.get(controlledUid);
            Relation relation = new Relation(++uidCounter, null, controlUid, controlType, controlledUid, controlledType,
                    Relation.Type.CONTROLLED, source);
//            networkDBAdaptor.addRelation(relation, tx);
            relations.add(relation);
        }
    }

    private void addSetAttributes(Set<String> input, String attrName, Node node) {
        if (input != null) {
            String value = StringUtils.join(input, ";");
            if (StringUtils.isNotEmpty(value)) {
                node.addAttribute(attrName, value);
            }
        }
    }

    private void addMapAttributes(Map<String, Object> map, String attrName, Node node) {
        if (map != null) {
            for (String key : map.keySet()) {
                Object value = map.get(key);
                if (value != null) {
                    node.addAttribute(attrName + key, value);
                }
            }
        }
    }

    private void addSetXref(Set<Xref> xrefs, Long uid, Node.Type type, Transaction tx) {
        for (Xref xref: xrefs) {
            String dbName = xref.getDb();
            String id = xref.getId();
            if (xref.getDb().toLowerCase().equals("gene ontology")) {
                dbName = "go";
                id = xref.getId().split(":")[1];
            }
            addXref(id, dbName, uid, type, tx);
        }
    }

    private void addXref(String xrefId, String dbName, Long uid, Node.Type type, Transaction tx) {
        if (filters != null && filters.containsKey(FilterField.XREF_DBNAME.name())
                && filters.get(FilterField.XREF_DBNAME.name()).contains(dbName)) {
            return;
        }

        Node xrefNode = new Node(uidCounter, xrefId, xrefId, Node.Type.XREF, source);
        xrefNode.addAttribute("dbName", dbName);
        nodes.add(xrefNode);
        //networkDBAdaptor.addNode(xrefNode, tx);
//        networkDBAdaptor.mergeNode(xrefNode, "id", "dbName", tx);
//
        Relation relation = new Relation(++uidCounter, null, uid, type, xrefNode.getUid(), xrefNode.getType(),
                Relation.Type.XREF, source);
//        networkDBAdaptor.addRelation(relation, tx);
        relations.add(relation);
    }

/*
//        Set<Xref> xrefs = physicalEntityBP.getXref();
//        for (Xref xref : xrefs) {
//            if (xref.getDb() != null) {
//                String source = xref.getDb().toLowerCase();
//                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
//                    physicalEntity.setOntology(new Ontology(xref.getDb(), xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
//                } else if (source.equals("pubmed")) {
//                    physicalEntity.setPublication(new Publication(xref.getDb(), xref.getId()));
//                } else {
//                    physicalEntity.setXref(new org.opencb.bionetdb.core.models.Xref(xref.getDb(),
//                            xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
//                }
//            }
//        }

 */

    private String getBioPaxId(String rdfId) {
        return rdfId.split("#")[1];
    }

    private void updateAuxMaps(Node node) {
        rdfToUidMap.put(node.getId(), node.getUid());
        uidToTypeMap.put(node.getUid(), node.getType());
    }
}
