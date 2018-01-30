package org.opencb.bionetdb.core.io;

import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.Model;
import org.biopax.paxtools.model.level3.*;
import org.biopax.paxtools.model.level3.Catalysis;
import org.biopax.paxtools.model.level3.Complex;
import org.biopax.paxtools.model.level3.Dna;
import org.biopax.paxtools.model.level3.Interaction;
import org.biopax.paxtools.model.level3.PhysicalEntity;
import org.biopax.paxtools.model.level3.Process;
import org.biopax.paxtools.model.level3.Protein;
import org.biopax.paxtools.model.level3.Rna;
import org.biopax.paxtools.model.level3.SmallMolecule;
import org.biopax.paxtools.model.level3.Xref;
import org.opencb.bionetdb.core.models.*;
import org.sqlite.util.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by imedina on 05/08/15.
 */

public class BioPaxParser {

    private String level;
    private int uidCounter;
    private Map<String, Integer> rdfToUidMap;
    private Map<Integer, Node> nodeMap;
    private Map<Integer, Relationship> relationshipMap;

    private static final String REACTOME_FEAT = "reactome.";

    public BioPaxParser(String level) {
        this.level = level;

        init();
    }

    private void init() {
        uidCounter = 0;
        rdfToUidMap = new HashMap<>();
        nodeMap = new HashMap<>();
        relationshipMap = new HashMap<>();
    }

    public Network parse(Path path) throws IOException {
        Network network = new Network();

        // Reading GZip input stream
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
        } else {
            inputStream = Files.newInputStream(path);
        }

        // Retrieving model from BioPAX file
        BioPAXIOHandler handler = new SimpleIOHandler();
        Model model = handler.convertFromOWL(inputStream);

        // Retrieving BioPAX elements
        Set<BioPAXElement> bioPAXElements = model.getObjects();

        // First loop to create all physical entity nodes
        for (BioPAXElement bioPAXElement: bioPAXElements) {
            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                // Physical Entities
                case "PhysicalEntity":
                case "Dna":
                case "Rna":
                case "Protein":
                case "Complex":
                case "SmallMolecule":
                    PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;
                    String peId = physicalEntityBP.getRDFId().split("#")[1];
                    rdfToUidMap.put(peId, uidCounter);
                    Node peNode = new Node(uidCounter);
                    peNode.setId(peId);
                    nodeMap.put(uidCounter, peNode);
                    uidCounter++;
                    break;
                case "BiochemicalReaction":
                case "TemplateReaction":
                case "Degradation":
                case "ComplexAssembly":
                case "MolecularInteraction":
                case "Transport":
                case "TransportWithBiochemicalReaction":
                case "Catalysis":
                case "Modulation":
                case "TemplateReactionRegulation":
                    Interaction interactionBP = (Interaction) bioPAXElement;
                    String interactionId = interactionBP.getRDFId().split("#")[1];
                    rdfToUidMap.put(interactionId, uidCounter);
                    Node intNode = new Node(uidCounter);
                    intNode.setId(interactionId);
                    nodeMap.put(uidCounter, intNode);
                    uidCounter++;
                    break;
                default:
                    break;

            }
        }

        // Second loop to add nodes and relationships to the network
        for (BioPAXElement bioPAXElement: bioPAXElements) {
            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                // Physical Entities
                case "PhysicalEntity":
                    addUndefinedEntity(bioPAXElement, network);
                    break;
                case "Dna":
                    addDna(bioPAXElement, network);
                    break;
                case "Rna":
                    addRna(bioPAXElement, network);
                    break;
                case "Protein":
                    addProtein(bioPAXElement, network);
                    break;
                case "Complex":
                    addComplex(bioPAXElement, network);
                    break;
                case "SmallMolecule":
                    addSmallMolecule(bioPAXElement, network);
                    break;

                // Interactions
                case "BiochemicalReaction":
                case "TemplateReaction":
                case "Degradation":
                case "ComplexAssembly":
                case "MolecularInteraction":
                case "Transport":
                case "TransportWithBiochemicalReaction":
                    addReaction(bioPAXElement, network);
                    break;
                case "Catalysis":
                    addCatalysis(bioPAXElement, network);
                    break;
                case "Modulation":
                case "TemplateReactionRegulation":
                    addRegulation(bioPAXElement, network);
                    break;
                default:
                    break;
            }
        }
        inputStream.close();
        return network;
    }

    //---------------------------------------------------------------
    //  B I O P A X     P H Y S I C A L     E N T I T I E S
    //---------------------------------------------------------------

    private void addUndefinedEntity(BioPAXElement bioPAXElement, Network network) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;

        // Common properties
        Node undefined = setPhysicalEntityCommonProperties(physicalEntityBP, network);

        // Add node to network
        network.addNode(undefined);
    }

    private void addDna(BioPAXElement bioPAXElement, Network network) {
        Dna dnaBP = (Dna) bioPAXElement;

        // Common properties
        Node dna = setPhysicalEntityCommonProperties(dnaBP, network);

        // Dna properties
        if (dnaBP.getEntityReference() != null) {

            EntityReference entityReference = dnaBP.getEntityReference();
            addXrefs(entityReference, dna.getUid(), network);

            // Description
            dna.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));
        }

        network.addNode(dna);
    }

    private void addRna(BioPAXElement bioPAXElement, Network network) {
        Rna rnaBP = (Rna) bioPAXElement;

        // Common properties
        Node rna = setPhysicalEntityCommonProperties(rnaBP, network);

        // Rna properties
        if (rnaBP.getEntityReference() != null) {

            EntityReference entityReference = rnaBP.getEntityReference();
            addXrefs(entityReference, rna.getUid(), network);

            // Description
            rna.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));
        }

        network.addNode(rna);
    }

    private void addProtein(BioPAXElement bioPAXElement, Network network) {
        Protein proteinBP = (Protein) bioPAXElement;

        // Common properties
        Node protein = setPhysicalEntityCommonProperties(proteinBP, network);

        // Protein properties
        if (proteinBP.getEntityReference() != null) {

            EntityReference entityReference = proteinBP.getEntityReference();
            addXrefs(entityReference, protein.getUid(), network);

            // Description
            protein.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));

            // Check for uniprot id
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref: xrefs) {
                if ("uniprot".equals(xref.getDb())) {
                    protein.setId(xref.getId());
                    break;
                }
            }
        }

        network.addNode(protein);
    }

    private void addSmallMolecule(BioPAXElement bioPAXElement, Network network) {
        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;

        // Common properties
        Node smallMolecule = setPhysicalEntityCommonProperties(smallMoleculeBP, network);

        // SmallMolecule properties
        if (smallMoleculeBP.getEntityReference() != null) {

            EntityReference entityReference = smallMoleculeBP.getEntityReference();
            addXrefs(entityReference, smallMolecule.getUid(), network);

            // description
            smallMolecule.addAttribute("description", StringUtils.join(new ArrayList<>(entityReference.getComment()), ";"));
        }

        network.addNode(smallMolecule);
    }

    private void addComplex(BioPAXElement bioPAXElement, Network network) {
        Complex complexBP = (Complex) bioPAXElement;

        // Common properties
        Node complex = setPhysicalEntityCommonProperties(complexBP, network);

        // Complex properties

        // Components
        Set<PhysicalEntity> components = complexBP.getComponent();
        for (PhysicalEntity component: components) {
            String id = component.getRDFId().split("#")[1];
            assert(rdfToUidMap.containsKey(id));
            int origUid = rdfToUidMap.get(id);
            Relationship relationship = new Relationship(uidCounter++, null, null, origUid, complex.getUid(),
                    Relationship.Type.COMPONENT_OF_COMPLEX);
            network.addRelationship(relationship);
        }

        // Stoichiometry
        List<Map<String, Object>> stoichiometry = new ArrayList<>();
        Set<Stoichiometry> stoichiometryItems = complexBP.getComponentStoichiometry();
        for (Stoichiometry stoichiometryItem: stoichiometryItems) {
            // component
            String id = stoichiometryItem.getPhysicalEntity().toString().split("#")[1];
            assert(rdfToUidMap.containsKey(id));
            int destUid = rdfToUidMap.get(id);
            Relationship relationship = new Relationship(uidCounter++, null, null, complex.getUid(), destUid,
                    Relationship.Type.STOICHIOMETRY);
            relationship.addAttribute("coefficient", stoichiometryItem.getStoichiometricCoefficient());
            network.addRelationship(relationship);
        }

        network.addNode(complex);
    }

    private void addXrefs(EntityReference entityReference, int sourceUid, Network network) {
        // Alternate IDs
        for (String name: entityReference.getName()) {
            Node x = new Node(uidCounter++);
            x.setId(name);
            x.setType(Node.Type.XREF);
            x.addAttribute("source", REACTOME_FEAT + "biopax");
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, sourceUid, x.getUid(),
                    Relationship.Type.XREF);
            network.addRelationship(relationship);
        }

        // Xref
        Set<Xref> xrefs = entityReference.getXref();
        for (Xref xref : xrefs) {
            Node x = new Node(uidCounter++);
            x.setId(xref.getId());
            x.setType(Node.Type.XREF);
            x.addAttribute("source", xref.getDb());
            x.addAttribute("sourceVersion", xref.getDbVersion());
            x.addAttribute("idVersion", xref.getIdVersion());
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, sourceUid, x.getUid(),
                    Relationship.Type.XREF);
            network.addRelationship(relationship);
        }
    }

    public void addXrefs(Set<Xref> xrefs, int uid, Network network) {
        for (Xref xref : xrefs) {
            if (xref.getDb() != null) {
                String source = xref.getDb().toLowerCase();
                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
                    //physicalEntity.setOntology(new Ontology(xref.getDb(), xref.getDbVersion(), xref.getId(), xref.getIdVersion()));
                    Node x = new Node(uidCounter++);
                    x.setId(xref.getId());
                    x.setType(Node.Type.ONTOLOGY);
                    x.addAttribute("source", source);
                    x.addAttribute("sourceVersion", xref.getDbVersion());
                    x.addAttribute("idVersion", xref.getIdVersion());
                    network.addNode(x);

                    Relationship relationship = new Relationship(uidCounter++, null, null, uid, x.getUid(),
                            Relationship.Type.ONTOLOGY);
                    network.addRelationship(relationship);
                } else if (!source.equals("pubmed")) {
                    Node x = new Node(uidCounter++);
                    x.setId(xref.getId());
                    x.setType(Node.Type.XREF);
                    x.addAttribute("source", source);
                    x.addAttribute("sourceVersion", xref.getDbVersion());
                    x.addAttribute("idVersion", xref.getIdVersion());
                    network.addNode(x);

                    Relationship relationship = new Relationship(uidCounter++, null, null, uid, x.getUid(),
                            Relationship.Type.XREF);
                    network.addRelationship(relationship);
                }
            }
        }
    }

    private Node setPhysicalEntityCommonProperties(PhysicalEntity physicalEntityBP, Network network) {
        // = SPECIFIC PROPERTIES =
        // id
        String id = physicalEntityBP.getRDFId().split("#")[1];
        assert(nodeMap.containsKey(id));
        Node physicalEntity = nodeMap.get(rdfToUidMap.get(id));

        // name
        if (physicalEntityBP.getDisplayName() != null) {
            physicalEntity.setName(physicalEntityBP.getDisplayName());

            Node x = new Node(uidCounter++);
            x.setId(physicalEntityBP.getDisplayName());
            x.setType(Node.Type.XREF);
            x.addAttribute("source", REACTOME_FEAT + "biopax");
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, physicalEntity.getUid(), x.getUid(),
                    Relationship.Type.XREF);
            network.addRelationship(relationship);
        }

        // altNames
        for (String name: physicalEntityBP.getName()) {
            Node x = new Node(uidCounter++);
            x.setId(name);
            x.setType(Node.Type.XREF);
            x.addAttribute("source", REACTOME_FEAT + "biopax");
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, physicalEntity.getUid(), x.getUid(),
                    Relationship.Type.XREF);
            network.addRelationship(relationship);
        }

        // cellularLocation
        CellularLocation cellularLocation = new CellularLocation();
        for (String name: physicalEntityBP.getCellularLocation().getTerm()) {
            Node x = new Node(uidCounter++);
            x.setId(name);
            x.setType(Node.Type.CELLULAR_LOCATION);
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, physicalEntity.getUid(), x.getUid(),
                    Relationship.Type.CELLULAR_LOCATION);
            network.addRelationship(relationship);
        }
        for (Xref cellLocXref: physicalEntityBP.getCellularLocation().getXref()) {
            Ontology ontology = new Ontology();
            if (cellLocXref.getDb().toLowerCase().equals("gene ontology")) {
                ontology.setSource("go");
                ontology.setId(cellLocXref.getId().split(":")[1]);
            } else {
                ontology.setSource(cellLocXref.getDb());
                ontology.setId(cellLocXref.getId());
            }
            ontology.setSourceVersion(cellLocXref.getDbVersion());
            ontology.setIdVersion(cellLocXref.getIdVersion());

            Node x = new Node(uidCounter++);
            x.setId(ontology.getId());
            x.setType(Node.Type.ONTOLOGY);
            x.addAttribute("source", ontology.getSource());
            x.addAttribute("sourceVersion", ontology.getSourceVersion());
            x.addAttribute("idVersion", ontology.getIdVersion());
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, physicalEntity.getUid(), x.getUid(),
                    Relationship.Type.ONTOLOGY);
            network.addRelationship(relationship);
        }

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

        // componentOfComplex
        for (Complex complex: physicalEntityBP.getComponentOf()) {
            String complexId = complex.getRDFId().split("#")[1];
            assert(rdfToUidMap.containsKey(complexId));
            int destUid = rdfToUidMap.get(complexId);
            Relationship relationship = new Relationship(uidCounter++, null, null, physicalEntity.getUid(), destUid,
                    Relationship.Type.COMPONENT_OF_COMPLEX);
            network.addRelationship(relationship);
        }

        // participantOfInteraction
//        for (Interaction interaction : physicalEntityBP.getParticipantOf()) {
//            physicalEntity.getParticipantOfInteraction().add(interaction.getRDFId().split("#")[1]);
//        }

        // xrefs
        addXrefs(physicalEntityBP.getXref(), physicalEntity.getUid(), network);

        // = NONSPECIFIC PROPERTIES =
        // comment
        physicalEntity.getAttributes().put(REACTOME_FEAT + "comment",
                physicalEntityBP.getComment());

        // availability
        physicalEntity.getAttributes().put(REACTOME_FEAT + "availability",
                physicalEntityBP.getAvailability());

        // annotations
        physicalEntity.getAttributes().put(REACTOME_FEAT + "annotations",
                physicalEntityBP.getAnnotations());

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

    private void addReaction(BioPAXElement bioPAXElement, Network network) {
        Interaction interactionBP = (Interaction) bioPAXElement;

        // Common Interaction properties
        Node reaction = setInteractionCommonProperties(interactionBP, network);

        String className = bioPAXElement.getModelInterface().getSimpleName();

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                // Setting up reaction type
                reaction.setType(Node.Type.REACTION);

                // TemplateReaction properties

                // Reactants
                if (templateReactBP.getTemplate() != null) {
                    String reactantId = templateReactBP.getTemplate().getRDFId().split("#")[1];
                    int reactantUid = rdfToUidMap.get(reactantId);

                    Relationship relationship = new Relationship(uidCounter++, null, null, reaction.getUid(), reactantUid,
                            Relationship.Type.REACTANT);
                    network.addRelationship(relationship);
                }

                // Products
                Set<PhysicalEntity> products = templateReactBP.getProduct();
                for (PhysicalEntity product: products) {
                    String productId = product.getRDFId().split("#")[1];
                    int productUid = rdfToUidMap.get(productId);

                    Relationship relationship = new Relationship(uidCounter++, null, null, reaction.getUid(), productUid,
                            Relationship.Type.PRODUCT);
                    network.addRelationship(relationship);
                }
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
                            addRelathionships(reaction.getUid(), leftItems, Relationship.Type.REACTANT, network);
                            addRelathionships(reaction.getUid(), rightItems, Relationship.Type.PRODUCT, network);
                            break;
                        case "RIGHT-TO-LEFT":
                        case "RIGHT_TO_LEFT":
                            addRelathionships(reaction.getUid(), rightItems, Relationship.Type.REACTANT, network);
                            addRelathionships(reaction.getUid(), leftItems, Relationship.Type.PRODUCT, network);
                            break;
                        default:
                            break;
                    }
                } else {
                    addRelathionships(reaction.getUid(), leftItems, Relationship.Type.REACTANT, network);
                    addRelathionships(reaction.getUid(), rightItems, Relationship.Type.PRODUCT, network);
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
                break;
        }

        network.addNode(reaction);
    }

    private void addCatalysis(BioPAXElement bioPAXElement, Network network) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;

        // Common Interaction properties
        Node catalysis = setInteractionCommonProperties(catalysisBP, network);

        // Catalysis properties

        // controllers
        Set<Controller> controllers = catalysisBP.getController();
        for (Controller controller: controllers) {
            String controllerId = controller.getRDFId().split("#")[1];
            int controllerUid = rdfToUidMap.get(controllerId);

            Relationship relationship = new Relationship(uidCounter++, null, null, catalysis.getUid(), controllerUid,
                    Relationship.Type.CONTROLLER);
            network.addRelationship(relationship);
        }

        // controlled
        Set<Process> controlledProcesses = catalysisBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            String controlledId = controlledProcess.getRDFId().split("#")[1];
            int controlledUid = rdfToUidMap.get(controlledId);

            Relationship relationship = new Relationship(uidCounter++, null, null, catalysis.getUid(), controlledUid,
                    Relationship.Type.CONTROLLED);
            network.addRelationship(relationship);
        }

        // controlType
        catalysis.setType(Node.Type.CATALYSIS);
        catalysis.addLabel(catalysisBP.getControlType().toString());

        // cofactor
        Set<PhysicalEntity> cofactors = catalysisBP.getCofactor();
        for (PhysicalEntity cofactor: cofactors) {
            String cofactorId = cofactor.getRDFId().split("#")[1];
            int cofactorUid = rdfToUidMap.get(cofactorId);

            Relationship relationship = new Relationship(uidCounter++, null, null, catalysis.getUid(), cofactorUid,
                    Relationship.Type.COFACTOR);
            network.addRelationship(relationship);
        }

        network.addNode(catalysis);
    }

    private void addRegulation(BioPAXElement bioPAXElement, Network network) {
        Control controlBP = (Control) bioPAXElement;

        // Common Interaction properties
        Node regulation = setInteractionCommonProperties(controlBP, network);

        // Regulation properties

        // controllers
        Set<Controller> controllers = controlBP.getController();
        for (Controller controller: controllers) {
            String controllerId = controller.getRDFId().split("#")[1];
            int controllerUid = rdfToUidMap.get(controllerId);

            Relationship relationship = new Relationship(uidCounter++, null, null, regulation.getUid(), controllerUid,
                    Relationship.Type.CONTROLLER);
            network.addRelationship(relationship);
        }

        // controlled
        Set<Process> controlledProcesses = controlBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            String controlledId = controlledProcess.getRDFId().split("#")[1];
            int controlledUid = rdfToUidMap.get(controlledId);

            Relationship relationship = new Relationship(uidCounter++, null, null, regulation.getUid(), controlledUid,
                    Relationship.Type.CONTROLLED);
            network.addRelationship(relationship);
        }

        // controlType
        regulation.setType(Node.Type.REGULATION);
        regulation.addLabel(controlBP.getControlType().toString());

        network.addNode(regulation);
    }

    private Node setInteractionCommonProperties(Interaction interactionBP, Network network) {
        // = SPECIFIC PROPERTIES =
        // id
        String id = interactionBP.getRDFId().split("#")[1];
        assert(nodeMap.containsKey(id));
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

            Node x = new Node(uidCounter++);
            x.setId(interactionBP.getDisplayName());
            x.setType(Node.Type.XREF);
            x.addAttribute("source", REACTOME_FEAT + "biopax");
            network.addNode(x);

            Relationship relationship = new Relationship(uidCounter++, null, null, interaction.getUid(), x.getUid(),
                    Relationship.Type.XREF);
            network.addRelationship(relationship);
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
        addXrefs(interactionBP.getXref(), interaction.getUid(), network);

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

        // = NONSPECIFIC PROPERTIES =
        // availability
        interaction.getAttributes().put(REACTOME_FEAT + "availability",
                interactionBP.getAvailability());

        // annotations
        interaction.getAttributes().put(REACTOME_FEAT + "annotations",
                interactionBP.getAnnotations());

        // interactionType
        interaction.getAttributes().put(REACTOME_FEAT + "interactionType",
                interactionBP.getInteractionType());

        return interaction;
    }

    private void addRelathionships(int origUid, List<String> destIds, Relationship.Type type, Network network) {
        for (String destId: destIds) {
            int destUid = rdfToUidMap.get(destId);

            Relationship relationship = new Relationship(uidCounter++, null, null, origUid, destUid, type);
            network.addRelationship(relationship);
        }
    }
}
