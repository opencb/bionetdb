package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.Model;
import org.biopax.paxtools.model.level3.*;
import org.biopax.paxtools.model.level3.Process;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Neo4JBioPaxLoader {

    private Neo4JNetworkDBAdaptor networkDBAdaptor;
    private String source;

    private Map<String, Long> rdfToUidMap;

    private static final String REACTOME_FEAT = "reactome.";

    public Neo4JBioPaxLoader(Neo4JNetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
        rdfToUidMap = new HashMap<>();
    }

    public void loadBioPaxFile(Path path) throws IOException {
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

        // First loop to create all physical entity nodes
        for (BioPAXElement bioPAXElement: bioPAXElements) {
            session.writeTransaction(tx -> {
                switch (bioPAXElement.getModelInterface().getSimpleName()) {
                    // Physical Entities
                    case "PhysicalEntity": {
                        Node node = loadUndefinedEntity(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());

                        updatePhysicalEntity(bioPAXElement, tx);
                        break;
                    }
                    case "Dna": {
                        Node node = loadDna(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());

                        updatePhysicalEntity(bioPAXElement, tx);
                        break;
                    }
                    case "Rna": {
                        Node node = loadRna(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());

                        updatePhysicalEntity(bioPAXElement, tx);
                        break;
                    }
                    case "Protein": {
                        Node node = loadProtein(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());

                        updatePhysicalEntity(bioPAXElement, tx);
                        break;
                    }
                    case "Complex": {
                        Node node = loadComplex(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());

                        updatePhysicalEntity(bioPAXElement, tx);
                        break;
                    }
                    case "SmallMolecule": {
                        Node node = loadSmallMolecule(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());

                        updatePhysicalEntity(bioPAXElement, tx);
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
                        Node node = loadReaction(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());
                        break;
                    }
                    case "Catalysis": {
                        Node node = loadCatalysis(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());
                        break;
                    }
                    case "Modulation":
                    case "TemplateReactionRegulation": {
                        Node node = loadRegulation(bioPAXElement, tx);
                        rdfToUidMap.put(node.getId(), node.getUid());
                        break;
                    }
                    default:
                        break;
                }
                return 1;
            });
        }

        // Second loop to create relationships between physical entity nodes
        for (BioPAXElement bioPAXElement: bioPAXElements) {
            session.writeTransaction(tx -> {
                switch (bioPAXElement.getModelInterface().getSimpleName()) {
                    case "Complex": {
                        updateComplex(bioPAXElement, tx);
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
                    case "Modulation":
                    case "TemplateReactionRegulation":
                        updateRegulation(bioPAXElement, tx);
                        break;
                    default:
                        break;
                }
                return 1;
            });
        }

        session.close();

        inputStream.close();
    }

    //-------------------------------------------------------------------------
    // NODE CREATION
    //-------------------------------------------------------------------------

    private Node loadUndefinedEntity(BioPAXElement bioPAXElement, Transaction tx) {
        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.UNDEFINED, source);

        // Common properties
        setPhysicalEntityCommonProperties(physicalEntityBP, node);

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadDna(BioPAXElement bioPAXElement, Transaction tx) {
        Dna dnaBP = (Dna) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.DNA, source);

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

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadRna(BioPAXElement bioPAXElement, Transaction tx) {
        Rna rnaBP = (Rna) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.RNA, source);

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

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadProtein(BioPAXElement bioPAXElement, Transaction tx) {
        Protein proteinBP = (Protein) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.PROTEIN, source);

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

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadSmallMolecule(BioPAXElement bioPAXElement, Transaction tx) {
        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.SMALL_MOLECULE, source);

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

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadComplex(BioPAXElement bioPAXElement, Transaction tx) {
        Complex complexBP = (Complex) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.COMPLEX, source);

        // Common properties
        setPhysicalEntityCommonProperties(complexBP, node);

        // Complex properties

        // Component node and stoichiometry are added later

        networkDBAdaptor.addNode(node, tx);
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

    private Node loadReaction(BioPAXElement bioPAXElement, Transaction tx) {
        String className = bioPAXElement.getModelInterface().getSimpleName();
        Node node = new Node(-1, null, null, Node.Type.REACTION, source);

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

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadCatalysis(BioPAXElement bioPAXElement, Transaction tx) {
        Catalysis catalysisBP = (Catalysis) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.CATALYSIS, source);

        // Common Interaction properties
        setInteractionCommonProperties(catalysisBP, node);

        // Catalysis properties

        // Controller relationships are added later
        // Controlled relationships are added later

        // ControlType
        node.addAttribute("controlType", catalysisBP.getControlType().toString());

        // Cofactor nodes/relationships are added later

        networkDBAdaptor.addNode(node, tx);
        return node;
    }

    private Node loadRegulation(BioPAXElement bioPAXElement, Transaction tx) {
        Control controlBP = (Control) bioPAXElement;
        Node node = new Node(-1, null, null, Node.Type.REGULATION, source);

        // Common Interaction properties
        setInteractionCommonProperties(controlBP, node);

        // Regulation properties

        // Controller relationships are added later
        // Controlled relationships are added later

        // ControlType
        node.addAttribute("controlType", controlBP.getControlType().toString());

        networkDBAdaptor.addNode(node, tx);
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

        // = SPECIFIC PROPERTIES =

        // name
        if (physicalEntityBP.getDisplayName() != null) {
            addXref(physicalEntityBP.getDisplayName(), REACTOME_FEAT + "biopax", physicalEntityUid, tx);
        }

        // altNames
        for (String name: physicalEntityBP.getName()) {
            addXref(name, REACTOME_FEAT + "biopax", physicalEntityUid, tx);
        }

        // cellularLocation
        Node cellularLocNode = null;
        for (String name: physicalEntityBP.getCellularLocation().getTerm()) {
            cellularLocNode = new Node(-1, null, name, Node.Type.CELLULAR_LOCATION, source);
            networkDBAdaptor.mergeNode(cellularLocNode, "name", tx);

            Relation relation = new Relation(-1, null, physicalEntityUid, cellularLocNode.getUid(),
                    Relation.Type.CELLULAR_LOCATION, source);
            networkDBAdaptor.addRelation(relation, tx);

            // Get the first term
            break;
        }
        if (cellularLocNode != null) {
            addSetXref(physicalEntityBP.getCellularLocation().getXref(), cellularLocNode.getUid(), tx);
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
        addSetXref(physicalEntityBP.getXref(), physicalEntityUid, tx);

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
            Relation relation = new Relation(-1, null, componentUid, complexUid, Relation.Type.COMPONENT_OF_COMPLEX, source);
            if (stoichiometryMap.containsKey(componentUid)) {
                relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(componentUid));
            }
            networkDBAdaptor.addRelation(relation, tx);
        }
    }

    private void updateReaction(BioPAXElement bioPAXElement, Transaction tx) {
        String className = bioPAXElement.getModelInterface().getSimpleName();

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                String templateReactId = getBioPaxId(templateReactBP.getRDFId());
                Long templateReactUid = rdfToUidMap.get(templateReactId);

                // TemplateReaction properties

                // Reactants
                if (templateReactBP.getTemplate() != null) {
                    Long reactantUid = rdfToUidMap.get(getBioPaxId(templateReactBP.getTemplate().getRDFId()));
                    Relation relation = new Relation(-1, null, templateReactUid, reactantUid, Relation.Type.REACTANT, source);
                    networkDBAdaptor.addRelation(relation, tx);
                }

                // Products
                Set<PhysicalEntity> products = templateReactBP.getProduct();
                for (PhysicalEntity product: products) {
                    Long productUid = rdfToUidMap.get(getBioPaxId(product.getRDFId()));
                    Relation relation = new Relation(-1, null, templateReactUid, productUid, Relation.Type.PRODUCT, source);
                    networkDBAdaptor.addRelation(relation, tx);
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
                        Relation relation = new Relation(-1, null, conversionUid, uid, type1, source);
                        if (stoichiometryMap.containsKey(id)) {
                            relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(id));
                        }
                        networkDBAdaptor.addRelation(relation, tx);
                    }

                    // Products
                    for (String id: rightItems) {
                        Long uid = rdfToUidMap.get(id);
                        Relation relation = new Relation(-1, null, conversionUid, uid, type2, source);
                        if (stoichiometryMap.containsKey(id)) {
                            relation.addAttribute("stoichiometricCoeff", stoichiometryMap.get(id));
                        }
                        networkDBAdaptor.addRelation(relation, tx);
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

        // Controllers
        Set<Controller> controllers = catalysisBP.getController();
        for (Controller controller: controllers) {
            Long controllerUid = rdfToUidMap.get(getBioPaxId(controller.getRDFId()));
            Relation relation = new Relation(-1, null, catalysisUid, controllerUid, Relation.Type.CONTROLLER, source);
            networkDBAdaptor.addRelation(relation, tx);
        }

        // Controlled
        Set<Process> controlledProcesses = catalysisBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            Long controlledUid = rdfToUidMap.get(getBioPaxId(controlledProcess.getRDFId()));
            Relation relation = new Relation(-1, null, catalysisUid, controlledUid, Relation.Type.CONTROLLED, source);
            networkDBAdaptor.addRelation(relation, tx);
        }

        // Cofactor
        Set<PhysicalEntity> cofactors = catalysisBP.getCofactor();
        for (PhysicalEntity cofactor: cofactors) {
            Long cofactorUid = rdfToUidMap.get(getBioPaxId(cofactor.getRDFId()));
            Relation relation = new Relation(-1, null, catalysisUid, cofactorUid, Relation.Type.COFACTOR, source);
            networkDBAdaptor.addRelation(relation, tx);
        }
    }

    private void updateRegulation(BioPAXElement bioPAXElement, Transaction tx) {
        Control controlBP = (Control) bioPAXElement;

        String controlId = getBioPaxId(controlBP.getRDFId());
        Long controlUid = rdfToUidMap.get(controlId);

        // Regulation properties

        // Controllers
        Set<Controller> controllers = controlBP.getController();
        for (Controller controller: controllers) {
            Long controllerUid = rdfToUidMap.get(getBioPaxId(controller.getRDFId()));
            Relation relation = new Relation(-1, null, controlUid, controllerUid, Relation.Type.CONTROLLER, source);
            networkDBAdaptor.addRelation(relation, tx);
        }

        // Controlled
        Set<Process> controlledProcesses = controlBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            Long controlledUid = rdfToUidMap.get(getBioPaxId(controlledProcess.getRDFId()));
            Relation relation = new Relation(-1, null, controlUid, controlledUid, Relation.Type.CONTROLLED, source);
            networkDBAdaptor.addRelation(relation, tx);
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

    private void addSetXref(Set<Xref> xrefs, Long uid, Transaction tx) {
        for (Xref xref: xrefs) {
            String dbName = xref.getDb();
            String id = xref.getId();
            if (xref.getDb().toLowerCase().equals("gene ontology")) {
                dbName = "go";
                id = xref.getId().split(":")[1];
            }
            addXref(id, dbName, uid, tx);
        }
    }

    private void addXref(String xrefId, String dbName, Long uid, Transaction tx) {
        Node xrefNode = new Node(-1, xrefId, xrefId, Node.Type.XREF, source);
        xrefNode.addAttribute("dbName", dbName);
        networkDBAdaptor.mergeNode(xrefNode, "id", "dbName", tx);

        Relation relation = new Relation(-1, null, uid, xrefNode.getUid(), Relation.Type.XREF, source);
        networkDBAdaptor.addRelation(relation, tx);
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
}