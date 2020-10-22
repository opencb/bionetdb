package org.opencb.bionetdb.lib.db;

import org.apache.commons.lang3.StringUtils;
import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.Model;
import org.biopax.paxtools.model.level3.*;
import org.biopax.paxtools.model.level3.Process;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.opencb.bionetdb.core.utils.CsvInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Neo4jBioPaxImporter {
    private static final String REACTOME_FEAT = "reactome.";
    private static final int TRANSACTION_BATCH_SIZE = 1000;

    private String source;
    private Map<String, Set<String>> filters;

    private Map<String, Long> rdfToUid;
    private Map<Long, Node.Type> uidToType;
    private Map<String, Long> protRdfIdToCelLocUid;
    private Set<String> celLocUidSet;

    private List<Node> nodes;
    private List<Relation> relations;

    private long nodeLoadingTime = 0;
    private long relationLoadingTime = 0;

    private CsvInfo csv;
    private BioPAXProcessing bioPAXProcessing;

    protected static Logger logger;

    public enum FilterField {
        XREF_DBNAME ("XREF_DBNAME");

        private final String filterField;

        FilterField(String filterField) {
            this.filterField = filterField;
        }
    }

    public interface BioPAXProcessing {
        String SEPARATOR = "___";
        void processNodes(List<Node> nodes);
        void processRelations(List<Relation> relations);
    }

    public Neo4jBioPaxImporter(CsvInfo csv, BioPAXProcessing bioPAXProcessing) {
        this(csv, null, bioPAXProcessing);
    }


    public Neo4jBioPaxImporter(CsvInfo csv, Map<String, Set<String>> filters, BioPAXProcessing bioPAXProcessing) {
        this.csv = csv;
        this.filters = filters;
        this.bioPAXProcessing = bioPAXProcessing;

        this.rdfToUid = new HashMap<>();
        this.uidToType = new HashMap<>();
        this.protRdfIdToCelLocUid = new HashMap<>();
        this.celLocUidSet = new HashSet<>();

        this.nodes = new ArrayList<>();
        this.relations = new ArrayList<>();

        this.nodeLoadingTime = 0;
        this.relationLoadingTime = 0;

        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    public void addReactomeFiles(List<File> files) throws IOException {
        for (File file: files) {
            importBioPaxFile(file.toPath());
        }
    }

    private void importBioPaxFile(Path path) throws IOException {
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
            //System.out.println(uid);
            node = null;
            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                // Physical Entities
                case "PhysicalEntity": {
                    node = loadUndefinedEntity(bioPAXElement);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement);
                    break;
                }
                case "Dna": {
                    node = loadDna(bioPAXElement);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement);
                    break;
                }
                case "Rna": {
                    String rnaName = ((Rna) bioPAXElement).getDisplayName();
                    if (isMicroRNA(rnaName)) {
                        // Process miRN
                        Long rnaUid = csv.getLong(rnaName);
                        if (rnaUid == null) {
                            // miRNA not registered yet
                            node = loadRna(bioPAXElement);
                            updateAuxMaps(node);

                            updatePhysicalEntity(bioPAXElement);
                            if (node.getName() != null) {
                                csv.putLong(node.getName(), node.getUid());
                            }
                        } else {
                            // miRNA already registered
                            // Get the RDF ID and save it to be referenced later for the possible relationships
                            String rnaRDFId = getBioPaxId(bioPAXElement.getRDFId());
                            updateAuxMaps(rnaRDFId, rnaUid, Node.Type.RNA);
                        }
                    } else {
                        node = loadRna(bioPAXElement);
                        updateAuxMaps(node);

                        updatePhysicalEntity(bioPAXElement);
                    }
                    break;
                }
                case "Protein": {
                    String protPrimaryId = null;
                    String protName = ((Protein) bioPAXElement).getDisplayName();
                    if (StringUtils.isEmpty(protName)) {
                        Set<Xref> xrefs = ((Protein)bioPAXElement).getXref();
                        for (Xref xref: xrefs) {
                            if (!StringUtils.containsIgnoreCase(xref.getDb(), "Reactome")) {
                                protPrimaryId = csv.getProteinCache().getPrimaryId(xref.getId());
                                if (StringUtils.isNotEmpty(protPrimaryId)) {
                                    break;
                                }
                            }
                        }
                        if (StringUtils.isEmpty(protPrimaryId)) {
                            protPrimaryId = getBioPaxId(bioPAXElement.getRDFId());
                        }
                    } else {
                        protPrimaryId = csv.getProteinCache().getPrimaryId(protName);
                        if (StringUtils.isEmpty(protPrimaryId)) {
                            Set<Xref> xrefs = ((Protein)bioPAXElement).getXref();
                            for (Xref xref: xrefs) {
                                if (!StringUtils.containsIgnoreCase(xref.getDb(), "Reactome")) {
                                    protPrimaryId = csv.getProteinCache().getPrimaryId(xref.getId());
                                    if (StringUtils.isNotEmpty(protPrimaryId)) {
                                        break;
                                    }
                                }
                            }
                        }
                        if (StringUtils.isEmpty(protPrimaryId)) {
                            protPrimaryId = protName;
                        }
                    }

                    Long protUid = (protPrimaryId == null ? null : csv.getLong(protPrimaryId));
                    if (protUid == null) {
                        node = loadProtein(bioPAXElement);
                        updateAuxMaps(node);

                        updatePhysicalEntity(bioPAXElement);
                        csv.putLong(protPrimaryId, node.getUid());
                    } else {
                        // The protein node exists, get the RDF ID and save it to be
                        // referenced later for the possible relationships (interaction, complex,...)
                        String protRDFId = getBioPaxId(bioPAXElement.getRDFId());
                        updateAuxMaps(protRDFId, protUid, Node.Type.PROTEIN);

                        // And create the cellular location node and relationship if necessary
                        Long celLocUid;
                        Node cellularLocNode;
                        for (String name: ((PhysicalEntity) bioPAXElement).getCellularLocation().getTerm()) {
                            celLocUid = csv.getLong("cl." + name);
                            if (celLocUid == null) {
                                cellularLocNode = new Node(csv.getAndIncUid(), null, name, Node.Type.CELLULAR_LOCATION,
                                        source);
                                nodes.add(cellularLocNode);
                                celLocUid = cellularLocNode.getUid();
                                csv.putLong("cl." + name, celLocUid);
                            }
                            protRdfIdToCelLocUid.put(protRDFId, celLocUid);

                            if (!celLocUidSet.contains(protUid + "." + celLocUid)) {
                                Relation relation = new Relation(csv.getAndIncUid(), null, protUid,
                                        Node.Type.PROTEIN, celLocUid, Node.Type.CELLULAR_LOCATION,
                                        Relation.Type.CELLULAR_LOCATION, source);
                                relations.add(relation);
                                celLocUidSet.add(protUid + "." + celLocUid);
                            }
                        }
                    }
                    break;
                }
                case "Complex": {
                    node = loadComplex(bioPAXElement);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement);
                    break;
                }
                case "SmallMolecule": {
                    node = loadSmallMolecule(bioPAXElement);
                    updateAuxMaps(node);

                    updatePhysicalEntity(bioPAXElement);
                    break;
                }

                // Pathways
                case "Pathway": {
                    node = loadPathway(bioPAXElement);
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
                    node = loadReaction(bioPAXElement);
                    updateAuxMaps(node);
                    break;
                }
                case "Catalysis": {
                    node = loadCatalysis(bioPAXElement);
                    updateAuxMaps(node);
                    break;
                }
                case "Control":
                case "Modulation":
                case "TemplateReactionRegulation": {
                    node = loadRegulation(bioPAXElement);
                    updateAuxMaps(node);
                    break;
                }
                default:
//                    logger.info("=====> skip entity {}", bioPAXElement.getModelInterface().getSimpleName());
                    break;
            }
            if (++numProcessedItems % 10000 == 0) {
                logger.info("1: " + Math.round(100. * numProcessedItems / numItems) + "%");
            }
            if (node != null) {
                nodes.add(node);
                if (nodes.size() >= TRANSACTION_BATCH_SIZE) {
                    numNodes += nodes.size();
                    bioPAXProcessing.processNodes(nodes);
                    nodes.clear();
                }
            }
            if (relations.size() > TRANSACTION_BATCH_SIZE) {
                numRelations += relations.size();
                bioPAXProcessing.processRelations(relations);
                relations.clear();
            }
        }
        if (nodes.size() > 0) {
            numNodes += nodes.size();
            bioPAXProcessing.processNodes(nodes);
        }

        if (relations.size() > TRANSACTION_BATCH_SIZE) {
            numRelations += relations.size();
            bioPAXProcessing.processRelations(relations);
            relations.clear();
        }

        // Second loop to create relationships between physical entity nodes
        numProcessedItems = 0;
        iterator = bioPAXElements.iterator();
        while (iterator.hasNext()) {
            BioPAXElement bioPAXElement = iterator.next();
            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                case "Complex": {
                    updateComplex(bioPAXElement);
                    break;
                }

                // Pathways
                case "Pathway": {
                    updatePathway(bioPAXElement);
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
                    updateReaction(bioPAXElement);
                    break;
                case "Catalysis":
                    updateCatalysis(bioPAXElement);
                    break;
                case "Control":
                case "Modulation":
                case "TemplateReactionRegulation":
                    updateRegulation(bioPAXElement);
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
                bioPAXProcessing.processRelations(relations);
                relations.clear();
            }
        }

        if (relations.size() > TRANSACTION_BATCH_SIZE) {
            numRelations += relations.size();
            bioPAXProcessing.processRelations(relations);
        }

        inputStream.close();

        logger.info("Processing {} containing {} BioPax elements in {} s", path, numItems, (System.currentTimeMillis() - startTime) / 1000);
        logger.info("Processing {} nodes", numNodes);
        logger.info("Processing {} relations", numRelations);
    }

    private String cleanValue(String value) {
        return value.replace("\"", ",").replace("\\", "|");
    }

    //-------------------------------------------------------------------------
    // NODE CREATION
    //-------------------------------------------------------------------------

    private Node loadUndefinedEntity(BioPAXElement bioPAXElement) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.UNDEFINED, source);

        // Common properties
        setPhysicalEntityCommonProperties(physicalEntityBP, node);

        return node;
    }

    private Node loadDna(BioPAXElement bioPAXElement) {
        Dna dnaBP = (Dna) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.DNA, source);

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

        return node;
    }

    private Node loadRna(BioPAXElement bioPAXElement) {
        Rna rnaBP = (Rna) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.RNA, source);

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

        return node;
    }

    private Node loadProtein(BioPAXElement bioPAXElement) {
        Protein proteinBP = (Protein) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.PROTEIN, source);

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

        return node;
    }

    private Node loadSmallMolecule(BioPAXElement bioPAXElement) {
        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.SMALL_MOLECULE, source);

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

        return node;
    }

    private Node loadComplex(BioPAXElement bioPAXElement) {
        Complex complexBP = (Complex) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.COMPLEX, source);

        // Common properties
        setPhysicalEntityCommonProperties(complexBP, node);

        // Complex properties

        // Component node and stoichiometry are added later

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
        // Comment
        addSetAttributes(physicalEntityBP.getComment(), "comment", physicalEntityNode);

        // Availability
        addSetAttributes(physicalEntityBP.getAvailability(), "availability", physicalEntityNode);
        if (physicalEntityBP.getAvailability() != null) {
            String availability = StringUtils.join(physicalEntityBP.getAvailability(), ";");
            if (StringUtils.isNotEmpty(availability)) {
                physicalEntityNode.addAttribute("availability", availability);
            }
        }

        // Annotations
        addMapAttributes(physicalEntityBP.getAnnotations(), "annot", physicalEntityNode);

        // Features
        int i = 0;
        for (EntityFeature entityFeature: physicalEntityBP.getFeature()) {
            String featureName = entityFeature.getModelInterface().getSimpleName();
            physicalEntityNode.addAttribute("feat" + i + "_type", featureName);
            physicalEntityNode.addAttribute("feat" + i + "_name", entityFeature.toString());
            i++;
        }
    }

    private Node loadPathway(BioPAXElement bioPAXElement) {
        Pathway pathwayBP = (Pathway) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), getBioPaxId(pathwayBP.getRDFId()), pathwayBP.getDisplayName(),
                Node.Type.PATHWAY, source);

        // Common properties
//        setPhysicalEntityCommonProperties(complexBP, node);

        return node;
    }

    private Node loadReaction(BioPAXElement bioPAXElement) {
        String className = bioPAXElement.getModelInterface().getSimpleName();
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.REACTION, source);

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

        return node;
    }

    private Node loadCatalysis(BioPAXElement bioPAXElement) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.CATALYSIS, source);

        // Common Interaction properties
        setInteractionCommonProperties(catalysisBP, node);

        // Catalysis properties

        // Controller relationships are added later
        // Controlled relationships are added later

        // ControlType
        node.addAttribute("controlType", catalysisBP.getControlType().toString());

        // Cofactor nodes/relationships are added later

        return node;
    }

    private Node loadRegulation(BioPAXElement bioPAXElement) {
        Control controlBP = (Control) bioPAXElement;
        Node node = new Node(csv.getAndIncUid(), null, null, Node.Type.REGULATION, source);

        // Common Interaction properties
        setInteractionCommonProperties(controlBP, node);

        // Regulation properties

        // Controller relationships are added later
        // Controlled relationships are added later

        // ControlType
        node.addAttribute("controlType", controlBP.getControlType().toString());

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

    private void updatePhysicalEntity(BioPAXElement bioPAXElement) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;

        String physicalEntityId = getBioPaxId(physicalEntityBP.getRDFId());
        Long physicalEntityUid = rdfToUid.get(physicalEntityId);
        Node.Type physicalEntityType = uidToType.get(physicalEntityUid);

        // = SPECIFIC PROPERTIES =

        // name
//        if (physicalEntityBP.getDisplayName() != null) {
//            addXref(physicalEntityBP.getDisplayName(), REACTOME_FEAT + "biopax", physicalEntityUid, physicalEntityType);
//        }

        // altNames
//        for (String name: physicalEntityBP.getName()) {
//            addXref(name, REACTOME_FEAT + "biopax", physicalEntityUid, physicalEntityType);
//        }

        // cellularLocation
        Node cellularLocNode = null;
        for (String name: physicalEntityBP.getCellularLocation().getTerm()) {
            Long celLocUid = csv.getLong("cl." + name);
            if (celLocUid == null) {
                cellularLocNode = new Node(csv.getAndIncUid(), null, name, Node.Type.CELLULAR_LOCATION, source);
                nodes.add(cellularLocNode);
                celLocUid = cellularLocNode.getUid();
                csv.putLong("cl." + name, celLocUid);
            }
            if (!celLocUidSet.contains(physicalEntityUid + "." + celLocUid)) {
                Relation relation = new Relation(csv.getAndIncUid(), null, physicalEntityUid,
                        uidToType.get(physicalEntityUid), celLocUid, Node.Type.CELLULAR_LOCATION,
                        Relation.Type.CELLULAR_LOCATION, source);
                relations.add(relation);
                celLocUidSet.add(physicalEntityUid + "." + celLocUid);
                if (physicalEntityType == Node.Type.PROTEIN) {
                    protRdfIdToCelLocUid.put(physicalEntityId, celLocUid);
                }
            }
        }


//        if (cellularLocNode != null) {
//            addSetXref(physicalEntityBP.getCellularLocation().getXref(), cellularLocNode.getUid(),
//                    cellularLocNode.getType());
//        }

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
//        addSetXref(physicalEntityBP.getXref(), physicalEntityUid, physicalEntityType);

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

    private void updateComplex(BioPAXElement bioPAXElement) {
        Complex complexBP = (Complex) bioPAXElement;

        String complexId = getBioPaxId(complexBP.getRDFId());
        Long complexUid = rdfToUid.get(complexId);
        Node.Type complexType = uidToType.get(complexUid);

        // Stoichiometry
        Map<Long, Float> stoichiometryMap = new HashMap<>();
        Set<Stoichiometry> stoichiometryItems = complexBP.getComponentStoichiometry();
        for (Stoichiometry stoichiometryItem: stoichiometryItems) {
            String peId = getBioPaxId(stoichiometryItem.getPhysicalEntity().getRDFId());
            Long peUid = rdfToUid.get(peId);
            stoichiometryMap.put(peUid, stoichiometryItem.getStoichiometricCoefficient());
        }

        // Components
        Set<PhysicalEntity> components = complexBP.getComponent();
        for (PhysicalEntity component: components) {
            Long componentUid = rdfToUid.get(getBioPaxId(component.getRDFId()));
            Node.Type componentType = uidToType.get(componentUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, componentUid, componentType, complexUid,
                    complexType, Relation.Type.COMPONENT_OF_COMPLEX, source);
            if (stoichiometryMap.containsKey(componentUid)) {
                relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(componentUid));
            }
            relations.add(relation);

            // Check to add cellular location
            checkProteinCellularLoc(getBioPaxId(component.getRDFId()), componentType, complexUid, complexType);
        }
    }


    private void updatePathway(BioPAXElement bioPAXElement) {
        Pathway pathwayBP = (Pathway) bioPAXElement;

        String pathwayId = getBioPaxId(pathwayBP.getRDFId());
        Long pathwayUid = rdfToUid.get(pathwayId);
        Node.Type pathwayType = uidToType.get(pathwayUid);

        // Components
        Set<Process> components = pathwayBP.getPathwayComponent();
        for (Process component: components) {
            Long componentUid = rdfToUid.get(getBioPaxId(component.getRDFId()));
            Node.Type componentType = uidToType.get(componentUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, componentUid, componentType, pathwayUid,
                    pathwayType, Relation.Type.COMPONENT_OF_PATHWAY, source);
            relations.add(relation);
        }

        //
        Set<PathwayStep> pathwayOrder = pathwayBP.getPathwayOrder();
        for (PathwayStep pathwayStep: pathwayOrder) {
            for (Process currentStep: pathwayStep.getStepProcess()) {
                Long currentStepUid = rdfToUid.get(getBioPaxId(currentStep.getRDFId()));
                Node.Type currentStepType = uidToType.get(currentStepUid);

                for (PathwayStep pathwayNextStep: pathwayStep.getNextStep()) {
                    for (Process nextStep: pathwayNextStep.getStepProcess()) {
                        if (rdfToUid.containsKey(getBioPaxId(nextStep.getRDFId()))) {
                            Long nextStepUid = rdfToUid.get(getBioPaxId(nextStep.getRDFId()));
                            Node.Type nextStepType = uidToType.get(nextStepUid);
                            try {
                                if (csv.getLong(currentStepUid + "." + nextStepUid) == null) {
                                    Relation relation = new Relation(csv.getAndIncUid(), null, currentStepUid,
                                            currentStepType, nextStepUid, nextStepType, Relation.Type.PATHWAY_NEXT_STEP,
                                            source);
                                    relations.add(relation);
                                    csv.putLong(currentStepUid + "." + nextStepUid, 1L);
                                }
                            } catch (Exception e) {
                                logger.info("impossible create realtionship: " + e.getMessage());
                                logger.info("current step: {}, uid {}, type {}", currentStep.getRDFId(), currentStepUid,
                                        currentStepType);
                                logger.info("next step   : {}, uid {}, type {}", nextStep.getRDFId(), nextStepUid,
                                        nextStepType);
                            }
                        }
                    }
                }
            }
        }
    }

    private void updateReaction(BioPAXElement bioPAXElement) {
        String className = bioPAXElement.getModelInterface().getSimpleName();

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                String templateReactId = getBioPaxId(templateReactBP.getRDFId());
                Long templateReactUid = rdfToUid.get(templateReactId);
                Node.Type templateReactType = uidToType.get(templateReactUid);

                // TemplateReaction properties

                // Reactants
                if (templateReactBP.getTemplate() != null) {
                    Long reactantUid = rdfToUid.get(getBioPaxId(templateReactBP.getTemplate().getRDFId()));
                    Node.Type reactantType = uidToType.get(reactantUid);
                    Relation relation = new Relation(csv.getAndIncUid(), null, templateReactUid,
                            templateReactType, reactantUid, reactantType, Relation.Type.REACTANT, source);
                    relations.add(relation);

                    // Check to add cellular location
                    checkProteinCellularLoc(getBioPaxId(templateReactBP.getTemplate().getRDFId()), reactantType,
                            templateReactUid, templateReactType);
                }

                // Products
                Set<PhysicalEntity> products = templateReactBP.getProduct();
                for (PhysicalEntity product: products) {
                    Long productUid = rdfToUid.get(getBioPaxId(product.getRDFId()));
                    Node.Type productType = uidToType.get(productUid);
                    Relation relation = new Relation(csv.getAndIncUid(), null, templateReactUid,
                            templateReactType, productUid, productType, Relation.Type.PRODUCT, source);
                    relations.add(relation);

                    // Check to add cellular location
                    checkProteinCellularLoc(getBioPaxId(product.getRDFId()), productType, templateReactUid,
                            templateReactType);
                }
                break;
            case "BiochemicalReaction":
            case "Degradation":
            case "ComplexAssembly":
            case "Transport":
            case "TransportWithBiochemicalReaction":
                Conversion conversionBP = (Conversion) bioPAXElement;

                String conversionId = getBioPaxId(conversionBP.getRDFId());
                Long conversionUid = rdfToUid.get(conversionId);
                Node.Type conversionType = uidToType.get(conversionUid);

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
                        Long uid = rdfToUid.get(id);
                        Node.Type type = uidToType.get(uid);
                        Relation relation = new Relation(csv.getAndIncUid(), null, conversionUid, conversionType,
                                uid, type, type1, source);
                        if (stoichiometryMap.containsKey(id)) {
                            relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(id));
                        }
                        relations.add(relation);

                        // Check to add cellular location
                        checkProteinCellularLoc(id, type, conversionUid, conversionType);
                    }

                    // Products
                    for (String id: rightItems) {
                        Long uid = rdfToUid.get(id);
                        Node.Type type = uidToType.get(uid);
                        Relation relation = new Relation(csv.getAndIncUid(), null, conversionUid, conversionType, uid, type, type2, source);
                        if (stoichiometryMap.containsKey(id)) {
                            relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(id));
                        }
                        relations.add(relation);

                        // Check to add cellular location
                        checkProteinCellularLoc(id, type, conversionUid, conversionType);
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

    private void updateCatalysis(BioPAXElement bioPAXElement) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;

        String catalysisId = getBioPaxId(catalysisBP.getRDFId());
        Long catalysisUid = rdfToUid.get(catalysisId);
        Node.Type catalysisType = uidToType.get(catalysisUid);

        // Controllers
        Set<Controller> controllers = catalysisBP.getController();
        for (Controller controller: controllers) {
            Long controllerUid = rdfToUid.get(getBioPaxId(controller.getRDFId()));
            Node.Type controllerType = uidToType.get(controllerUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, catalysisUid, catalysisType, controllerUid,
                    controllerType, Relation.Type.CONTROLLER, source);
            relations.add(relation);

            // Check to add cellular location
            checkProteinCellularLoc(getBioPaxId(controller.getRDFId()), controllerType, catalysisUid, catalysisType);
        }

        // Controlled
        Set<Process> controlledProcesses = catalysisBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            Long controlledUid = rdfToUid.get(getBioPaxId(controlledProcess.getRDFId()));
            Node.Type controlledType = uidToType.get(controlledUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, catalysisUid, catalysisType, controlledUid,
                    controlledType, Relation.Type.CONTROLLED, source);
            relations.add(relation);

            // Check to add cellular location
            checkProteinCellularLoc(getBioPaxId(controlledProcess.getRDFId()), controlledType, catalysisUid,
                    catalysisType);
        }

        // Cofactor
        Set<PhysicalEntity> cofactors = catalysisBP.getCofactor();
        for (PhysicalEntity cofactor: cofactors) {
            Long cofactorUid = rdfToUid.get(getBioPaxId(cofactor.getRDFId()));
            Node.Type cofactorType = uidToType.get(cofactorUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, catalysisUid, catalysisType, cofactorUid,
                    cofactorType, Relation.Type.COFACTOR, source);
            relations.add(relation);

            // Check to add cellular location
            checkProteinCellularLoc(getBioPaxId(cofactor.getRDFId()), cofactorType, catalysisUid, catalysisType);
        }
    }

    private void updateRegulation(BioPAXElement bioPAXElement) {
        Control controlBP = (Control) bioPAXElement;

        String controlId = getBioPaxId(controlBP.getRDFId());
        Long controlUid = rdfToUid.get(controlId);
        Node.Type controlType = uidToType.get(controlUid);

        // Regulation properties

        // Controllers
        Set<Controller> controllers = controlBP.getController();
        for (Controller controller: controllers) {
            Long controllerUid = rdfToUid.get(getBioPaxId(controller.getRDFId()));
            Node.Type controllerType = uidToType.get(controllerUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, controlUid, controlType, controllerUid,
                    controllerType, Relation.Type.CONTROLLER, source);
            relations.add(relation);

            // Check to add cellular location
            checkProteinCellularLoc(getBioPaxId(controller.getRDFId()), controllerType, controlUid, controlType);
        }

        // Controlled
        Set<Process> controlledProcesses = controlBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            Long controlledUid = rdfToUid.get(getBioPaxId(controlledProcess.getRDFId()));
            Node.Type controlledType = uidToType.get(controlledUid);
            Relation relation = new Relation(csv.getAndIncUid(), null, controlUid, controlType, controlledUid,
                    controlledType, Relation.Type.CONTROLLED, source);
            relations.add(relation);

            // Check to add cellular location
            checkProteinCellularLoc(getBioPaxId(controlledProcess.getRDFId()), controlledType, controlUid, controlType);
        }
    }

    private void checkProteinCellularLoc(String proteinRdfId, Node.Type proteinType, long targetUid, Node.Type targetType) {
        if (proteinType == Node.Type.PROTEIN) {
            if (protRdfIdToCelLocUid.containsKey(proteinRdfId)) {
                long celLocUid = protRdfIdToCelLocUid.get(proteinRdfId);
                if (!celLocUidSet.contains(targetUid + "." + celLocUid)) {
                    Relation relation = new Relation(csv.getAndIncUid(), null, targetUid, targetType, celLocUid,
                            Node.Type.CELLULAR_LOCATION, Relation.Type.CELLULAR_LOCATION, source);
                    relations.add(relation);
                    celLocUidSet.add(targetUid + "." + celLocUid);
                }
            }
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

    @Deprecated
    private void addSetXref(Set<Xref> xrefs, Long uid, Node.Type type) {
        for (Xref xref: xrefs) {
            String dbName = xref.getDb();
            String id = xref.getId();
            if (xref.getDb().toLowerCase().equals("gene ontology")) {
                dbName = "go";
                id = xref.getId().split(":")[1];
            }
            addXref(id, dbName, uid, type);
        }
    }

    @Deprecated
    private void addXref(String xrefId, String dbName, Long uid, Node.Type type) {
        if (filters != null && filters.containsKey(Neo4JBioPaxLoader.FilterField.XREF_DBNAME.name())
                && filters.get(Neo4JBioPaxLoader.FilterField.XREF_DBNAME.name()).contains(dbName)) {
            return;
        }

        // Only process XREF for proteins
        if (type == Node.Type.PROTEIN) {
            Long xrefUid = csv.getLong("x0." + xrefId);
            if (xrefUid == null) {
                Node xrefNode = new Node(csv.getAndIncUid(), xrefId, xrefId, Node.Type.XREF, source);
                xrefNode.addAttribute("dbName", dbName);
                nodes.add(xrefNode);

                xrefUid = xrefNode.getUid();
                csv.putLong("x0." + xrefId, xrefUid);
                csv.putLong("x1." + xrefId, uid);
            }

            Relation relation = new Relation(csv.getAndIncUid(), null, uid, type, xrefUid, Node.Type.XREF,
                    Relation.Type.XREF, source);
            relations.add(relation);
        } else if (type == Node.Type.RNA) {
            // mix = miRNA Xref, mixt = miRNA Xref target
            Long xrefUid = csv.getLong("mix." + xrefId);
            if (xrefUid == null) {
                Node xrefNode = new Node(csv.getAndIncUid(), xrefId, xrefId, Node.Type.XREF, source);
                xrefNode.addAttribute("dbName", dbName);
                nodes.add(xrefNode);

                xrefUid = xrefNode.getUid();
                csv.putLong("mix." + xrefId, xrefUid);
                csv.putLong("mixt." + xrefId, uid);
            }

            Relation relation = new Relation(csv.getAndIncUid(), null, uid, type, xrefUid,  Node.Type.XREF,
                    Relation.Type.XREF, source);
            relations.add(relation);
        }
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
        rdfToUid.put(node.getId(), node.getUid());
        uidToType.put(node.getUid(), node.getType());
    }

    private void updateAuxMaps(String rdfId, Long uid, Node.Type type) {
        rdfToUid.put(rdfId, uid);
        uidToType.put(uid, type);
    }

    private boolean isMicroRNA(String rnaName) {
        return (StringUtils.isNotEmpty(rnaName) && rnaName.startsWith("miR-"));
    }
}
