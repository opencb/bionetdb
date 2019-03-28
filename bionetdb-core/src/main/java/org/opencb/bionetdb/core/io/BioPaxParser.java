package org.opencb.bionetdb.core.io;

import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.level3.*;
import org.biopax.paxtools.model.level3.Process;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.sqlite.util.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by imedina on 05/08/15.
 */

public class BioPaxParser {

    private Network network;

    private String level;
    private String source;
    private long uidCounter;
    private Map<String, Long> rdfToUidMap;
    private Map<Long, Node> nodeMap;
    private Map<Long, Relation> relationshipMap;

    private Map<String, Map<String, Long>> reusedUidMap;

    private static final String REACTOME_FEAT = "reactome.";

    public BioPaxParser(String level) {
        this.level = level;

        init();
    }

    private void init() {
        network = new Network();

        uidCounter = 0;
        rdfToUidMap = new HashMap<>();
        nodeMap = new HashMap<>();
        relationshipMap = new HashMap<>();

        reusedUidMap = new HashMap<>();
    }

    public Network parse(Path path) throws IOException {
//        // Reading GZip input stream
//        InputStream inputStream;
//        if (path.toFile().getName().endsWith(".gz")) {
//            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
//        } else {
//            inputStream = Files.newInputStream(path);
//        }
//
//        this.source = path.toFile().getName();
//
//        // Retrieving model from BioPAX file
//        BioPAXIOHandler handler = new SimpleIOHandler();
//        Model model = handler.convertFromOWL(inputStream);
//
//        // Retrieving BioPAX elements
//        Set<BioPAXElement> bioPAXElements = model.getObjects();
//
//        // First loop to create all physical entity nodes
//        for (BioPAXElement bioPAXElement: bioPAXElements) {
//            switch (bioPAXElement.getModelInterface().getSimpleName()) {
//                // Physical Entities
//                case "PhysicalEntity":
//                case "Dna":
//                case "Rna":
//                case "Protein":
//                case "Complex":
//                case "SmallMolecule":
//                    PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;
//                    String peId = physicalEntityBP.getRDFId().split("#")[1];
//                    rdfToUidMap.put(peId, uidCounter);
//                    Node peNode = new Node(uidCounter);
//                    peNode.setId(peId);
//                    nodeMap.put(uidCounter, peNode);
//                    uidCounter++;
//                    break;
//                case "BiochemicalReaction":
//                case "TemplateReaction":
//                case "Degradation":
//                case "ComplexAssembly":
//                case "MolecularInteraction":
//                case "Transport":
//                case "TransportWithBiochemicalReaction":
//                case "Catalysis":
//                case "Modulation":
//                case "TemplateReactionRegulation":
//                    Interaction interactionBP = (Interaction) bioPAXElement;
//                    String interactionId = interactionBP.getRDFId().split("#")[1];
//                    rdfToUidMap.put(interactionId, uidCounter);
//                    Node intNode = new Node(uidCounter);
//                    intNode.setId(interactionId);
//                    nodeMap.put(uidCounter, intNode);
//                    uidCounter++;
//                    break;
//                default:
//                    break;
//
//            }
//        }
//
//        // Second loop to add nodes and relationships to the network
//        for (BioPAXElement bioPAXElement: bioPAXElements) {
//            switch (bioPAXElement.getModelInterface().getSimpleName()) {
//                // Physical Entities
//                case "PhysicalEntity":
//                    addUndefinedEntity(bioPAXElement);
//                    break;
//                case "Dna":
//                    addDna(bioPAXElement);
//                    break;
//                case "Rna":
//                    addRna(bioPAXElement);
//                    break;
//                case "Protein":
//                    addProtein(bioPAXElement);
//                    break;
//                case "Complex":
//                    addComplex(bioPAXElement);
//                    break;
//                case "SmallMolecule":
//                    addSmallMolecule(bioPAXElement);
//                    break;
//
//                // Interactions
//                case "BiochemicalReaction":
//                case "TemplateReaction":
//                case "Degradation":
//                case "ComplexAssembly":
//                case "MolecularInteraction":
//                case "Transport":
//                case "TransportWithBiochemicalReaction":
//                    addReaction(bioPAXElement);
//                    break;
//                case "Catalysis":
//                    addCatalysis(bioPAXElement);
//                    break;
//                case "Modulation":
//                case "TemplateReactionRegulation":
//                    addRegulation(bioPAXElement);
//                    break;
//                default:
//                    break;
//            }
//        }
//        inputStream.close();
//        return network;
        return null;
    }

    //---------------------------------------------------------------
    //  B I O P A X     P H Y S I C A L     E N T I T I E S
    //---------------------------------------------------------------

    private void addUndefinedEntity(BioPAXElement bioPAXElement) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;

        // Common properties
        Node undefined = setPhysicalEntityCommonProperties(physicalEntityBP);

        undefined.setType(Node.Type.UNDEFINED);

        // Add node to network
        network.addNode(undefined);
    }

    private void addDna(BioPAXElement bioPAXElement) {
        Dna dnaBP = (Dna) bioPAXElement;

        // Common properties
        Node dna = setPhysicalEntityCommonProperties(dnaBP);
        if (dna == null) {
            return;
        }

        dna.setType(Node.Type.DNA);

        // Dna properties
        if (dnaBP.getEntityReference() != null) {

            EntityReference entityReference = dnaBP.getEntityReference();
            addXrefs(entityReference, dna.getUid());

            // Description
            dna.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));
        }

        network.addNode(dna);
    }

    private void addRna(BioPAXElement bioPAXElement) {
        Rna rnaBP = (Rna) bioPAXElement;

        // Common properties
        Node rna = setPhysicalEntityCommonProperties(rnaBP);
        if (rna == null) {
            return;
        }

        rna.setType(Node.Type.RNA);

        // Rna properties
        if (rnaBP.getEntityReference() != null) {

            EntityReference entityReference = rnaBP.getEntityReference();
            addXrefs(entityReference, rna.getUid());

            // Description
            rna.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));
        }

        network.addNode(rna);
    }

    private void addProtein(BioPAXElement bioPAXElement) {
        Protein proteinBP = (Protein) bioPAXElement;

        // Common properties
        Node protein = setPhysicalEntityCommonProperties(proteinBP);
        if (protein == null) {
            return;
        }

        protein.setType(Node.Type.PROTEIN);

        // Protein properties
        if (proteinBP.getEntityReference() != null) {

            EntityReference entityReference = proteinBP.getEntityReference();
            addXrefs(entityReference, protein.getUid());

            // Description
            protein.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));

            // Check for uniprot id
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref: xrefs) {
                if (xref.getDb() != null && ("uniprot".equals(xref.getDb().toLowerCase()))) {
                    protein.setId(xref.getId());
                    protein.addAttribute("biopaxId", proteinBP.getRDFId().split("#")[1]);
                    break;
                }
            }
        }

        network.addNode(protein);
    }

    private void addSmallMolecule(BioPAXElement bioPAXElement) {
        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;

        // Common properties
        Node smallMolecule = setPhysicalEntityCommonProperties(smallMoleculeBP);
        if (smallMolecule == null) {
            return;
        }

        smallMolecule.setType(Node.Type.SMALL_MOLECULE);

        // SmallMolecule properties
        if (smallMoleculeBP.getEntityReference() != null) {

            EntityReference entityReference = smallMoleculeBP.getEntityReference();
            addXrefs(entityReference, smallMolecule.getUid());

            // description
            smallMolecule.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));
        }

        network.addNode(smallMolecule);
    }

    private void addComplex(BioPAXElement bioPAXElement) {
        Complex complexBP = (Complex) bioPAXElement;

        // Common properties
        Node complex = setPhysicalEntityCommonProperties(complexBP);
        if (complex == null) {
            return;
        }

        complex.setType(Node.Type.COMPLEX);

        // Complex properties

        // TODO: check if it is done in setPhysicalEntityCommonProperties when managing PhysicalEntity.getComponentOf()
//        // Components
//        Set<PhysicalEntity> components = complexBP.getComponent();
//        for (PhysicalEntity component: components) {
//            String id = component.getRDFId().split("#")[1];
//            if (!rdfToUidMap.containsKey(id)) {
//                System.out.println("Component ID " + id + " does not exist!!");
//            } else {
//                int origUid = rdfToUidMap.get(id);
//                Relation relation = new Relation(uidCounter++, null, origUid, complex.getUid(),
//                        Relation.Type.COMPONENT_OF_COMPLEX);
//                network.addRelation(relation);
//            }
//        }

        // Stoichiometry
        List<Map<String, Object>> stoichiometry = new ArrayList<>();
        Set<Stoichiometry> stoichiometryItems = complexBP.getComponentStoichiometry();
        for (Stoichiometry stoichiometryItem: stoichiometryItems) {
            // component
            String id = stoichiometryItem.getPhysicalEntity().toString().split("#")[1];
            if (!rdfToUidMap.containsKey(id)) {
                System.out.println("Stoichiometry Item ID " + id + " does not exist!!");
            } else {
                long destUid = rdfToUidMap.get(id);
//                Relation relation = new Relation(uidCounter++, null, complex.getUid(), complex.getType(), destUid,
// Relation.Type.STOICHIOMETRY);
//                relation.addAttribute("coefficient", stoichiometryItem.getStoichiometricCoefficient());
//                network.addRelation(relation);
            }
        }

        network.addNode(complex);
    }

    private void addXrefs(EntityReference entityReference, long origUid) {
//        // Alternate IDs
//        for (String name: entityReference.getName()) {
//            Node x = reuseXref(name);
//
//            Relation relation = new Relation(uidCounter++, null, origUid, x.getUid(), Relation.Type.XREF);
//            network.addRelation(relation);
//        }
//
//        // Xref
//        Set<Xref> xrefs = entityReference.getXref();
//        for (Xref xref : xrefs) {
//            Node x = reuseXref(xref);
//
//            Relation relation = new Relation(uidCounter++, null, origUid, x.getUid(), Relation.Type.XREF);
//            network.addRelation(relation);
//        }
    }

    public void addXrefs(Set<Xref> xrefs, long origUid) {
//        for (Xref xref : xrefs) {
//            if (xref.getDb() != null) {
//                String source = xref.getDb().toLowerCase();
//                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
//                    //physicalEntity.setOntology(new Ontology(xref.getDb(), xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
//
//                    Node x = reuseOntology(xref);
//
//                    Relation relation = new Relation(uidCounter++, null, origUid, x.getUid(), Relation.Type.ONTOLOGY);
//                    network.addRelation(relation);
//                } else if (!source.equals("pubmed")) {
//                    Node x = reuseXref(xref);
//
//                    Relation relation = new Relation(uidCounter++, null, origUid, x.getUid(), Relation.Type.XREF);
//                    network.addRelation(relation);
//                }
//            }
//        }
    }

    private Node setPhysicalEntityCommonProperties(PhysicalEntity physicalEntityBP) {
        // = SPECIFIC PROPERTIES =
        // id
        String id = physicalEntityBP.getRDFId().split("#")[1];
        if (!rdfToUidMap.containsKey(id)) {
            System.out.println("ID " + id + " does not exist!!");
            return null;
        }
        if (!nodeMap.containsKey(rdfToUidMap.get(id))) {
            System.out.println("ID " + id + ":" + rdfToUidMap.get(id) + " does not exist!!");
            return null;
        }
        Node physicalEntity = nodeMap.get(rdfToUidMap.get(id));

//        // name
//        if (physicalEntityBP.getDisplayName() != null) {
//            physicalEntity.setName(physicalEntityBP.getDisplayName());
//
//            Node x = reuseXref(physicalEntityBP.getDisplayName());
//
//            Relation relation = new Relation(uidCounter++, null, physicalEntity.getUid(), x.getUid(), Relation.Type.XREF);
//            network.addRelation(relation);
//        }
//
//        // altNames
//        for (String name: physicalEntityBP.getName()) {
//            Node x = reuseXref(name);
//
//            Relation relation = new Relation(uidCounter++, null, physicalEntity.getUid(), x.getUid(), Relation.Type.XREF);
//            network.addRelation(relation);
//        }

//        // cellularLocation
//        for (String name: physicalEntityBP.getCellularLocation().getTerm()) {
//            Node x = reuseCellularLocation(name);
//
//            Relation relation = new Relation(uidCounter++, null, physicalEntity.getUid(), x.getUid(), Relation.Type.CELLULAR_LOCATION);
//            network.addRelation(relation);
//        }
//        for (Xref cellLocXref: physicalEntityBP.getCellularLocation().getXref()) {
//            Node x = reuseOntology(cellLocXref);
//
//            Relation relation = new Relation(uidCounter++, null, physicalEntity.getUid(), x.getUid(), Relation.Type.ONTOLOGY);
//            network.addRelation(relation);
//        }

        // source
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

        // members
//        for (PhysicalEntity pe : physicalEntityBP.getMemberPhysicalEntity()) {
//            physicalEntity.getMembers().add(pe.getRDFId().split("#")[1]);
//        }

        // memberOfSet
//        for (PhysicalEntity peOf : physicalEntityBP.getMemberPhysicalEntityOf()) {
//            physicalEntity.getMemberOfSet().add(peOf.getRDFId().split("#")[1]);
//        }

//        // componentOfComplex
//        for (Complex complex: physicalEntityBP.getComponentOf()) {
//            String complexId = complex.getRDFId().split("#")[1];
//            if (!rdfToUidMap.containsKey(complexId)) {
//                System.out.println("Complex ID " + id + " does not exist, componentOfComplex!!");
//            } else {
//                long destUid = rdfToUidMap.get(complexId);
//                Relation relation = new Relation(uidCounter++, null, physicalEntity.getUid(), destUid,
// Relation.Type.COMPONENT_OF_COMPLEX);
//                network.addRelation(relation);
//            }
//        }

        // participantOfInteraction
//        for (Interaction interaction : physicalEntityBP.getParticipantOf()) {
//            physicalEntity.getParticipantOfInteraction().add(interaction.getRDFId().split("#")[1]);
//        }

        // xrefs
        addXrefs(physicalEntityBP.getXref(), physicalEntity.getUid());

        // = NONSPECIFIC PROPERTIES =
        // comment
//        physicalEntity.getAttributes().put(REACTOME_FEAT + "comment",
//                physicalEntityBP.getComment());
//
//        // availability
//        physicalEntity.getAttributes().put(REACTOME_FEAT + "availability",
//                physicalEntityBP.getAvailability());
//
//        // annotations
//        physicalEntity.getAttributes().put(REACTOME_FEAT + "annotations",
//                physicalEntityBP.getAnnotations());

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

        return physicalEntity;
    }

    //---------------------------------------------------------------
    //  B I O P A X     I N T E R A C T I O N S
    //---------------------------------------------------------------

    private void addReaction(BioPAXElement bioPAXElement) {
        Interaction interactionBP = (Interaction) bioPAXElement;

        // Common Interaction properties
        Node reaction = setInteractionCommonProperties(interactionBP);
        if (reaction == null) {
            return;
        }

        String className = bioPAXElement.getModelInterface().getSimpleName();

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                // Setting up reaction type
                reaction.setType(Node.Type.REACTION);

                // TemplateReaction properties

//                // Reactants
//                if (templateReactBP.getTemplate() != null) {
//                    String reactantId = templateReactBP.getTemplate().getRDFId().split("#")[1];
//                    long reactantUid = rdfToUidMap.get(reactantId);
//
//                    Relation relation = new Relation(uidCounter++, null, reaction.getUid(), reactantUid, Relation.Type.REACTANT);
//                    network.addRelation(relation);
//                }
//
//                // Products
//                Set<PhysicalEntity> products = templateReactBP.getProduct();
//                for (PhysicalEntity product: products) {
//                    String productId = product.getRDFId().split("#")[1];
//                    long productUid = rdfToUidMap.get(productId);
//
//                    Relation relation = new Relation(uidCounter++, null, reaction.getUid(), productUid, Relation.Type.PRODUCT);
//                    network.addRelation(relation);
//                }
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
                        reaction.setType(Node.Type.REACTION);
                        break;
                    case "ComplexAssembly":
                        reaction.setType(Node.Type.ASSEMBLY);
                        break;
                    case "Transport":
                    case "TransportWithBiochemicalReaction":
                        reaction.setType(Node.Type.TRANSPORT);
                        break;
                    default:
                        reaction.setType(Node.Type.INTERACTION);
                        break;
                }

                // Left items
                List<String> leftItems = new ArrayList<>();
                Set<PhysicalEntity> lefts = conversionBP.getLeft();
                for (PhysicalEntity left : lefts) {
                    leftItems.add(left.getRDFId().split("#")[1]);
                }

                // Right items
                List<String> rightItems = new ArrayList<>();
                Set<PhysicalEntity> rights = conversionBP.getRight();
                for (PhysicalEntity right : rights) {
                    rightItems.add(right.getRDFId().split("#")[1]);
                }

                if (conversionBP.getConversionDirection() != null) {
                    switch (conversionBP.getConversionDirection().toString()) {
                        case "REVERSIBLE":
                            reaction.addAttribute("reversible", true);
                            // NO BREAK HERE
                        case "LEFT-TO-RIGHT":
                        case "LEFT_TO_RIGHT":
                            addRelathionships(reaction.getUid(), leftItems, Relation.Type.REACTANT);
                            addRelathionships(reaction.getUid(), rightItems, Relation.Type.PRODUCT);
                            break;
                        case "RIGHT-TO-LEFT":
                        case "RIGHT_TO_LEFT":
                            addRelathionships(reaction.getUid(), rightItems, Relation.Type.REACTANT);
                            addRelathionships(reaction.getUid(), leftItems, Relation.Type.PRODUCT);
                            break;
                        default:
                            break;
                    }
                } else {
                    addRelathionships(reaction.getUid(), leftItems, Relation.Type.REACTANT);
                    addRelathionships(reaction.getUid(), rightItems, Relation.Type.PRODUCT);
                }

                // Spontaneous
                if (conversionBP.getSpontaneous() != null) {
                    reaction.addAttribute("spontaneous", conversionBP.getSpontaneous());
                }

//                // Stoichiometry
//                List<Map<String, Object>> stoichiometry = new ArrayList<>();
//                Set<Stoichiometry> stoichiometryItems = conversionBP.getParticipantStoichiometry();
//                for (Stoichiometry stoichiometryItem : stoichiometryItems) {
//                    Map<String, Object> stchmtr = new HashMap<>();
//                    stchmtr.put("component", stoichiometryItem.getPhysicalEntity().toString().split("#")[1]);
//                    stchmtr.put("coefficient", stoichiometryItem.getStoichiometricCoefficient());
//                    stoichiometry.add(stchmtr);
//                }
//                reaction.setStoichiometry(stoichiometry);
//
//                // Adding EC number to xrefs
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
                reaction.setType(Node.Type.ASSEMBLY);

                break;
            default:
                reaction.setType(Node.Type.INTERACTION);
                break;
        }

        network.addNode(reaction);
    }

    private void addCatalysis(BioPAXElement bioPAXElement) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;

        // Common Interaction properties
        Node catalysis = setInteractionCommonProperties(catalysisBP);
        if (catalysis == null) {
            return;
        }

        // Catalysis properties

        // controllers
//        Set<Controller> controllers = catalysisBP.getController();
//        for (Controller controller: controllers) {
//            String controllerId = controller.getRDFId().split("#")[1];
//            long controllerUid = rdfToUidMap.get(controllerId);
//
//            Relation relation = new Relation(uidCounter++, null, catalysis.getUid(), controllerUid, Relation.Type.CONTROLLER);
//            network.addRelation(relation);
//        }
//
//        // controlled
//        Set<Process> controlledProcesses = catalysisBP.getControlled();
//        for (Process controlledProcess: controlledProcesses) {
//            String controlledId = controlledProcess.getRDFId().split("#")[1];
//            long controlledUid = rdfToUidMap.get(controlledId);
//
//            Relation relation = new Relation(uidCounter++, null, catalysis.getUid(), controlledUid, Relation.Type.CONTROLLED);
//            network.addRelation(relation);
//        }

        // controlType
        catalysis.setType(Node.Type.CATALYSIS);
        catalysis.addTag(catalysisBP.getControlType().toString());

        // cofactor
        Set<PhysicalEntity> cofactors = catalysisBP.getCofactor();
        for (PhysicalEntity cofactor: cofactors) {
            String cofactorId = cofactor.getRDFId().split("#")[1];
            long cofactorUid = rdfToUidMap.get(cofactorId);

//            Relation relation = new Relation(uidCounter++, null, catalysis.getUid(), cofactorUid, Relation.Type.COFACTOR);
//            network.addRelation(relation);
        }

        network.addNode(catalysis);
    }

    private void addRegulation(BioPAXElement bioPAXElement) {
        Control controlBP = (Control) bioPAXElement;

        // Common Interaction properties
        Node regulation = setInteractionCommonProperties(controlBP);
        if (regulation == null) {
            return;
        }

        // Regulation properties

        // controllers
        Set<Controller> controllers = controlBP.getController();
        for (Controller controller: controllers) {
            String controllerId = controller.getRDFId().split("#")[1];
            long controllerUid = rdfToUidMap.get(controllerId);

//            Relation relation = new Relation(uidCounter++, null, regulation.getUid(), controllerUid, Relation.Type.CONTROLLER);
//            network.addRelation(relation);
        }

        // controlled
        Set<Process> controlledProcesses = controlBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            String controlledId = controlledProcess.getRDFId().split("#")[1];
            long controlledUid = rdfToUidMap.get(controlledId);

//            Relation relation = new Relation(uidCounter++, null, regulation.getUid(), controlledUid, Relation.Type.CONTROLLED);
//            network.addRelation(relation);
        }

        // controlType
        regulation.setType(Node.Type.REGULATION);
        regulation.addTag(controlBP.getControlType().toString());

        network.addNode(regulation);
    }

    private Node setInteractionCommonProperties(Interaction interactionBP) {
        // = SPECIFIC PROPERTIES =
        // id
        String id = interactionBP.getRDFId().split("#")[1];
        if (!rdfToUidMap.containsKey(id)) {
            System.out.println("ID " + id + " does not exist!! (Interaction node)!!");
            return null;
        }
        if (!nodeMap.containsKey(rdfToUidMap.get(id))) {
            System.out.println("ID " + id + ":" + rdfToUidMap.get(id) + " does not exist (Interaction node)!!");
            return null;
        }
        Node interaction = nodeMap.get(rdfToUidMap.get(id));

        // description
        List<String> descs = new ArrayList<>();
        if (interactionBP.getComment() != null) {
            for (String comment : interactionBP.getComment()) {
                if (!comment.matches("(Authored:|Edited:|Reviewed:).+")) {
                    descs.add(comment);
                }
            }
            interaction.addAttribute("description", StringUtils.join(descs, ";"));
        }

        // name
        if (interactionBP.getDisplayName() != null) {
            interaction.setName(interactionBP.getDisplayName());

            Node x = reuseXref(interactionBP.getDisplayName());

//            Relation relation = new Relation(uidCounter++, null, interaction.getUid(), x.getUid(), Relation.Type.XREF);
//            network.addRelation(relation);
        }

        // source
//        List<String> sources = new ArrayList<>();
//        for (Provenance provenance : interactionBP.getDataSource()) {
//            sources.addAll(provenance.getName());
//        }
//        interaction.setSource(sources);

        // participants
//        for (Entity entity : interactionBP.getParticipant()) {
//            interaction.getParticipants().add(entity.getRDFId().split("#")[1]);
//        }

        // controlledBy
//        for (Control control : interactionBP.getControlledOf()) {
//            interaction.getControlledBy().add(control.getRDFId().split("#")[1]);
//        }

        // processOfPathway
//        for (PathwayStep pathwayStep : interactionBP.getStepProcessOf()) {
//            interaction.getProcessOfPathway().add(pathwayStep.getPathwayOrderOf().getRDFId().split("#")[1]);
//        }

        // xrefs
        addXrefs(interactionBP.getXref(), interaction.getUid());

//        // publications
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
//                interaction.setPublication(publication);
//            }
//        }

//        // = NONSPECIFIC PROPERTIES =
//        // availability
//        interaction.getAttributes().put(REACTOME_FEAT + "availability",
//                interactionBP.getAvailability());
//
//        // annotations
//        interaction.getAttributes().put(REACTOME_FEAT + "annotations",
//                interactionBP.getAnnotations());
//
//        // interactionType
//        interaction.getAttributes().put(REACTOME_FEAT + "interactionType",
//                interactionBP.getInteractionType());

        return interaction;
    }

    //---------------------------------------------------------------
    //  C O M M O N
    //---------------------------------------------------------------

    private void addRelathionships(long origUid, List<String> destIds, Relation.Type type) {
        for (String destId: destIds) {
            if (!rdfToUidMap.containsKey(destId)) {
                System.out.println("Destination Node ID " + destId + " does not exist!!!");
                continue;
            }
            long destUid = rdfToUidMap.get(destId);

//            Relation relation = new Relation(uidCounter++, null, origUid, destUid, type);
//            network.addRelation(relation);
        }
    }

    private long getReuseUid(String type, String id) {
        if (!reusedUidMap.containsKey(type)) {
            reusedUidMap.put(type, new HashMap<>());
        }
        if (reusedUidMap.get(type).containsKey(id)) {
            return reusedUidMap.get(type).get(id);
        } else {
            return -1;
        }
    }

    private Node reuseXref(Xref x) {
        Node xref = new Node(0);
        long uid = getReuseUid(Node.Type.XREF.name(), x.getId());
        if (uid < 0) {
            xref.setUid(uidCounter++);

            xref.setId(x.getId());
            xref.setType(Node.Type.XREF);
            xref.addAttribute("source", x.getDb());
            xref.addAttribute("sourceVersion", x.getDbVersion());
            xref.addAttribute("idVersion", x.getIdVersion());

            nodeMap.put(xref.getUid(), xref);
            reusedUidMap.get(Node.Type.XREF.name()).put(xref.getId(), xref.getUid());
            network.addNode(xref);
        } else {
            xref = nodeMap.get(uid);
        }

        return xref;
    }

    private Node reuseXref(String name) {
        Node xref = new Node(0);
        long uid = getReuseUid(Node.Type.XREF.name(), name);
        if (uid < 0) {
            xref.setUid(uidCounter++);

            xref.setId(name);
            xref.setType(Node.Type.XREF);
            xref.addAttribute("source", REACTOME_FEAT + "biopax");

            nodeMap.put(xref.getUid(), xref);
            reusedUidMap.get(Node.Type.XREF.name()).put(name, xref.getUid());
            network.addNode(xref);
        } else {
            xref = nodeMap.get(uid);
        }

        return xref;
    }

    private Node reuseOntology(Xref xref) {
        Node ontology = new Node(0);
        long uid = getReuseUid(Node.Type.ONTOLOGY.name(), xref.getId());
        if (uid < 0) {
            ontology.setUid(uidCounter++);

//            if (cellLocXref.getDb().toLowerCase().equals("gene ontology")) {
//                x.setId(cellLocXref.getId().split(":")[1]);
//                x.addAttribute("source", "go");
//            } else {
//                x.setId(cellLocXref.getId());
//                x.addAttribute("source", cellLocXref.getDb());
//            }
//            // Update Uid for this node
//            x.setUid(updateReuseUidMap(Node.Type.ONTOLOGY.name(), x.getId()));
//            x.setType(Node.Type.ONTOLOGY);
//            x.addAttribute("sourceVersion", cellLocXref.getDbVersion());
//            x.addAttribute("idVersion", cellLocXref.getIdVersion());

            ontology.setId(xref.getId());
            ontology.setType(Node.Type.ONTOLOGY);
            ontology.addAttribute("source", source);
            ontology.addAttribute("sourceVersion", xref.getDbVersion());
            ontology.addAttribute("idVersion", xref.getIdVersion());

            nodeMap.put(ontology.getUid(), ontology);
            reusedUidMap.get(Node.Type.ONTOLOGY.name()).put(xref.getId(), ontology.getUid());
            network.addNode(ontology);
        } else {
            ontology = nodeMap.get(uid);
        }
        return ontology;
    }

    private Node reuseCellularLocation(String name) {
        Node celLocation = new Node(0);
        long uid = getReuseUid(Node.Type.CELLULAR_LOCATION.name(), name);
        if (uid < 0) {
            celLocation.setUid(uidCounter++);

            celLocation.setId(name);
            celLocation.setType(Node.Type.CELLULAR_LOCATION);

            nodeMap.put(celLocation.getUid(), celLocation);
            reusedUidMap.get(Node.Type.CELLULAR_LOCATION.name()).put(name, celLocation.getUid());
            network.addNode(celLocation);
        } else {
            celLocation = nodeMap.get(uid);
        }
        return celLocation;
    }
}
