package org.opencb.bionetdb.core.io;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final String REACTOME_FEAT = "reactome.";

    public BioPaxParser(String level) {
        this.level = level;

        init();
    }

    private void init() {
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

        for (BioPAXElement bioPAXElement : bioPAXElements) {

            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                // Physical Entities
                case "PhysicalEntity":
                    network.setPhysicalEntity(createUndefinedEntity(bioPAXElement));
                    break;
                case "Dna":
                    network.setPhysicalEntity(createDna(bioPAXElement));
                    break;
                case "Rna":
                    network.setPhysicalEntity(createRna(bioPAXElement));
                    break;
                case "Protein":
                    network.setPhysicalEntity(createProtein(bioPAXElement));
                    break;
                case "Complex":
                    network.setPhysicalEntity(createComplex(bioPAXElement));
                    break;
                case "SmallMolecule":
                    network.setPhysicalEntity(createSmallMolecule(bioPAXElement));
                    break;

                // Interactions
                case "BiochemicalReaction":
                case "TemplateReaction":
                case "Degradation":
                case "ComplexAssembly":
                case "MolecularInteraction":
                case "Transport":
                case "TransportWithBiochemicalReaction":
                    network.setInteraction(createReaction(bioPAXElement));
                    break;
                case "Catalysis":
                    network.setInteraction(createCatalysis(bioPAXElement));
                    break;
                case "Modulation":
                case "TemplateReactionRegulation":
                    network.setInteraction(createRegulation(bioPAXElement));
                    break;
            }

        }
        inputStream.close();
        return network;
    }

    private org.opencb.bionetdb.core.models.PhysicalEntity createUndefinedEntity(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.UndefinedEntity undefinedEntity;
        undefinedEntity = new UndefinedEntity();

        PhysicalEntity physicalEntityBP = (PhysicalEntity) bioPAXElement;

        // Common properties
        setPhysicalEntityCommonProperties(physicalEntityBP, undefinedEntity);

        return undefinedEntity;
    }

    private org.opencb.bionetdb.core.models.Dna createDna(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Dna dna = new org.opencb.bionetdb.core.models.Dna();
        Dna dnaBP = (Dna) bioPAXElement;

        // Common properties
        setPhysicalEntityCommonProperties(dnaBP, dna);

        // Dna properties
        if (dnaBP.getEntityReference() != null) {

            EntityReference entityReference = dnaBP.getEntityReference();

            // altIds
            for (String name : entityReference.getName()) {
                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
                xref.setSource(REACTOME_FEAT + "biopax");
                xref.setId(name);
                dna.setXref(xref);
            }

            // description
            dna.getDescription().addAll(entityReference.getComment());


            // xref
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref : xrefs) {
                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
                x.setSource(xref.getDb());
                x.setSourceVersion(xref.getDbVersion());
                x.setId(xref.getId());
                x.setIdVersion(xref.getIdVersion());
                dna.setXref(x);
            }
        }
        return dna;
    }

    private org.opencb.bionetdb.core.models.Rna createRna(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Rna rna = new org.opencb.bionetdb.core.models.Rna();
        Rna rnaBP = (Rna) bioPAXElement;

        // Common properties
        setPhysicalEntityCommonProperties(rnaBP, rna);

        // Rna properties
        if (rnaBP.getEntityReference() != null) {

            EntityReference entityReference = rnaBP.getEntityReference();

            // altIds
            for (String name : entityReference.getName()) {
                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
                xref.setSource(REACTOME_FEAT + "biopax");
                xref.setId(name);
                rna.setXref(xref);
            }

            // description
            rna.getDescription().addAll(entityReference.getComment());

            // xref
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref : xrefs) {
                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
                x.setSource(xref.getDb());
                x.setSourceVersion(xref.getDbVersion());
                x.setId(xref.getId());
                x.setIdVersion(xref.getIdVersion());
                rna.setXref(x);
            }
        }
        return rna;
    }

    private org.opencb.bionetdb.core.models.Protein createProtein(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Protein protein = new org.opencb.bionetdb.core.models.Protein();
        Protein proteinBP = (Protein) bioPAXElement;

        // Common properties
        setPhysicalEntityCommonProperties(proteinBP, protein);

        // Protein properties
        if (proteinBP.getEntityReference() != null) {

            EntityReference entityReference = proteinBP.getEntityReference();

            // altIds
            for (String name : entityReference.getName()) {
                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
                xref.setSource(REACTOME_FEAT + "biopax");
                xref.setId(name);
                protein.setXref(xref);
            }

            // description
            protein.getDescription().addAll(entityReference.getComment());

            // xref
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref : xrefs) {
                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
                x.setSource(xref.getDb());
                x.setSourceVersion(xref.getDbVersion());
                x.setId(xref.getId());
                x.setIdVersion(xref.getIdVersion());
                protein.setXref(x);
            }
        }
        return protein;
    }

    private org.opencb.bionetdb.core.models.SmallMolecule createSmallMolecule(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.SmallMolecule smallMolecule =
                new org.opencb.bionetdb.core.models.SmallMolecule();
        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;

        // Common properties
        setPhysicalEntityCommonProperties(smallMoleculeBP, smallMolecule);

        // SmallMolecule properties
        if (smallMoleculeBP.getEntityReference() != null) {

            EntityReference entityReference = smallMoleculeBP.getEntityReference();

            // altIds
            for (String name : entityReference.getName()) {
                org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
                xref.setSource(REACTOME_FEAT + "biopax");
                xref.setId(name);
                smallMolecule.setXref(xref);
            }

            // description
            smallMolecule.getDescription().addAll(entityReference.getComment());

            // xref
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref : xrefs) {
                org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
                x.setSource(xref.getDb());
                x.setSourceVersion(xref.getDbVersion());
                x.setId(xref.getId());
                x.setIdVersion(xref.getIdVersion());
                smallMolecule.setXref(x);
            }
        }
        return smallMolecule;
    }

    private org.opencb.bionetdb.core.models.Complex createComplex(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Complex complex = new org.opencb.bionetdb.core.models.Complex();
        Complex complexBP = (Complex) bioPAXElement;

        // Common properties
        setPhysicalEntityCommonProperties(complexBP, complex);

        // Complex properties

        // Components
        Set<PhysicalEntity> components = complexBP.getComponent();
        for (PhysicalEntity component : components) {
            complex.getComponents().add(component.getRDFId().split("#")[1]);
        }

        // Stoichiometry
        List<Map<String, Object>> stoichiometry = new ArrayList<>();
        Set<Stoichiometry> stoichiometryItems = complexBP.getComponentStoichiometry();
        for (Stoichiometry stoichiometryItem : stoichiometryItems) {
            Map<String, Object> stchmtr = new HashMap<>();
            stchmtr.put("component", stoichiometryItem.getPhysicalEntity().toString().split("#")[1]);
            stchmtr.put("coefficient", stoichiometryItem.getStoichiometricCoefficient());
            stoichiometry.add(stchmtr);
        }
        complex.setStoichiometry(stoichiometry);

        return complex;
    }

    private void setPhysicalEntityCommonProperties(PhysicalEntity physicalEntityBP,
                                      org.opencb.bionetdb.core.models.PhysicalEntity physicalEntity) {
        // = SPECIFIC PROPERTIES =

        // id
        physicalEntity.setId(physicalEntityBP.getRDFId().split("#")[1]);

        // name
        if (physicalEntityBP.getDisplayName() != null) {
            physicalEntity.setName(physicalEntityBP.getDisplayName());
        }

        // altNames
        for (String name : physicalEntityBP.getName()) {
            org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
            xref.setSource(REACTOME_FEAT + "biopax");
            xref.setId(name);
            physicalEntity.setXref(xref);
        }


        // cellularLocation
        CellularLocation cellularLocation = new CellularLocation();
        CellularLocationVocabulary cellularLocationVocabulary = physicalEntityBP.getCellularLocation();
        for (String name : cellularLocationVocabulary.getTerm()) {
            cellularLocation.setName(name);
        }
        for (Xref cellLocXref : cellularLocationVocabulary.getXref()) {
            Ontology ontology= new Ontology();
            if (cellLocXref.getDb().toLowerCase().equals("gene ontology")) {
                ontology.setSource("go");
                ontology.setId(cellLocXref.getId().split(":")[1]);
            } else {
                ontology.setSource(cellLocXref.getDb());
                ontology.setId(cellLocXref.getId());
            }
            ontology.setSourceVersion(cellLocXref.getDbVersion());
            ontology.setIdVersion(cellLocXref.getIdVersion());
            cellularLocation.setOntology(ontology);
        }
        physicalEntity.getCellularLocation().add(cellularLocation);

        // source
        List<String> sources = new ArrayList<>();
        Set<Provenance> provenances = physicalEntityBP.getDataSource();
        for (Provenance provenance : provenances) {
            sources.addAll(provenance.getName());
        }
        physicalEntity.setSource(sources);

        // TODO [WARNING]: MemberPhysicalEntity and MemberPhysicalEntityOf
        // Please avoid using this property in your BioPAX L3 models unless absolutely sure/required,
        // for there is an alternative way (using PhysicalEntity/entityReference/memberEntityReference),
        // and this will probably be deprecated in the future BioPAX releases.
        // http://www.biopax.org/m2site/paxtools-4.2.0/apidocs/org/biopax/paxtools/impl/level3/PhysicalEntityImpl.html

        // members
        Set<PhysicalEntity> pes = physicalEntityBP.getMemberPhysicalEntity();
        for (PhysicalEntity pe : pes) {
            physicalEntity.getMembers().add(pe.getRDFId().split("#")[1]);
        }

        // memberOfSet
        Set<PhysicalEntity> pesOf = physicalEntityBP.getMemberPhysicalEntityOf();
        for (PhysicalEntity peOf : pesOf) {
            physicalEntity.getMemberOfSet().add(peOf.getRDFId().split("#")[1]);
        }

        // componentOfComplex
        Set<Complex> complexes = physicalEntityBP.getComponentOf();
        for (Complex complex : complexes) {
            physicalEntity.getComponentOfComplex().add(complex.getRDFId().split("#")[1]);
        }

        // participantOfInteraction
        Set<Interaction> interactions = physicalEntityBP.getParticipantOf();
        for (Interaction interaction : interactions) {
            physicalEntity.getParticipantOfInteraction().add(interaction.getRDFId().split("#")[1]);
        }

        Set<Xref> xrefs = physicalEntityBP.getXref();
        for (Xref xref : xrefs) {
            if (xref.getDb() != null) {
                String source = xref.getDb().toLowerCase();
                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
                    Ontology ontology = new Ontology();
                    ontology.setSource(xref.getDb());
                    ontology.setSourceVersion(xref.getDbVersion());
                    ontology.setId(xref.getId());
                    ontology.setIdVersion(xref.getIdVersion());
                    physicalEntity.setOntology(ontology);
                } else if (source.equals("pubmed")) {
                    Publication publication = new Publication();
                    publication.setSource(xref.getDb());
                    publication.setId(xref.getId());
                    physicalEntity.setPublication(publication);
                } else {
                    org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
                    x.setSource(xref.getDb());
                    x.setSourceVersion(xref.getDbVersion());
                    x.setId(xref.getId());
                    x.setIdVersion(xref.getIdVersion());
                    physicalEntity.setXref(x);
                }
            }
        }

        // publications
        for (Evidence evidence : physicalEntityBP.getEvidence()) {
            for (Xref xref : evidence.getXref()) {
                PublicationXref pubXref = (PublicationXref) xref;
                Publication publication = new Publication();
                publication.setSource(pubXref.getDb());
                publication.setId(pubXref.getId());
                publication.setTitle(pubXref.getTitle());
                publication.setYear(pubXref.getYear());
                for (String author : pubXref.getAuthor()) {
                    publication.setAuthor(author);
                }
                for (String source : pubXref.getSource()) {
                    publication.setJournal(source);
                }
                physicalEntity.setPublication(publication);
            }
        }

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

        // features
        List<Map<String, Object>> features = new ArrayList<>();
        Set<EntityFeature> entityFeatures = physicalEntityBP.getFeature();
        for (EntityFeature entityFeature : entityFeatures) {
            Map<String, Object> feature = new HashMap<>();
            String featureName = entityFeature.getModelInterface().getSimpleName();
            feature.put("type", featureName);
            feature.put("name", entityFeature.toString());
            features.add(feature);
        }
        physicalEntity.setFeatures(features);
    }

    private org.opencb.bionetdb.core.models.Reaction createReaction(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Reaction reaction = new org.opencb.bionetdb.core.models.Reaction();

        String className = bioPAXElement.getModelInterface().getSimpleName();

        switch (className) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

                // Setting up reaction type
                reaction.setReactionType(Reaction.ReactionType.REACTION);

                // Common Interaction properties
                setInteractionCommonProperties(templateReactBP, reaction);

                // TemplateReaction properties

                // Reactants
                if(templateReactBP.getTemplate() != null){
                    reaction.getReactants().add(templateReactBP.getTemplate().getRDFId().split("#")[1]);
                }

                // Products
                Set<PhysicalEntity> products = templateReactBP.getProduct();
                for (PhysicalEntity product : products) {
                    reaction.getProducts().add(product.getRDFId().split("#")[1]);
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
                        reaction.setReactionType(Reaction.ReactionType.REACTION);
                        break;
                    case "ComplexAssembly":
                        reaction.setReactionType(Reaction.ReactionType.ASSEMBLY);
                        break;
                    case "Transport":
                    case "TransportWithBiochemicalReaction":
                        reaction.setReactionType(Reaction.ReactionType.TRANSPORT);
                        break;
                }

                // Common Interaction properties
                setInteractionCommonProperties(conversionBP, reaction);

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
                            reaction.setReversible(true);
                            // NO BREAK HERE
                        case "LEFT-TO-RIGHT":
                        case "LEFT_TO_RIGHT":
                            reaction.setReactants(leftItems);
                            reaction.setProducts(rightItems);
                            break;
                        case "RIGHT-TO-LEFT":
                        case "RIGHT_TO_LEFT":
                            reaction.setReactants(rightItems);
                            reaction.setProducts(leftItems);
                            break;
                    }
                } else {
                    reaction.setReactants(leftItems);
                    reaction.setProducts(rightItems);
                }

                // Spontaneous
                if (conversionBP.getSpontaneous() != null) {
                    if (conversionBP.getSpontaneous()) {
                        reaction.setSpontaneous(true);
                    } else {
                        reaction.setSpontaneous(false);
                    }
                }

                // Stoichiometry
                List<Map<String, Object>> stoichiometry = new ArrayList<>();
                Set<Stoichiometry> stoichiometryItems = conversionBP.getParticipantStoichiometry();
                for (Stoichiometry stoichiometryItem : stoichiometryItems) {
                    Map<String, Object> stchmtr = new HashMap<>();
                    stchmtr.put("component", stoichiometryItem.getPhysicalEntity().toString().split("#")[1]);
                    stchmtr.put("coefficient", stoichiometryItem.getStoichiometricCoefficient());
                    stoichiometry.add(stchmtr);
                }
                reaction.setStoichiometry(stoichiometry);

                // Adding EC number to xrefs
                if (className.equals("BiochemicalReaction")) {
                    BiochemicalReaction br = (BiochemicalReaction) bioPAXElement;
                    for (String ecNumber : br.getECNumber()) {
                        Ontology ontology = new Ontology();
                        ontology.setSource("ec");
                        ontology.setId(ecNumber);
                        reaction.setOntology(ontology);
                    }
                }

                break;
            case "MolecularInteraction":
                MolecularInteraction molecularInteractionBP = (MolecularInteraction) bioPAXElement;

                // Setting up reaction type
                reaction.setReactionType(Reaction.ReactionType.ASSEMBLY);

                // Common Interaction properties
                setInteractionCommonProperties(molecularInteractionBP, reaction);
                break;
        }
        return reaction;
    }

    private org.opencb.bionetdb.core.models.Catalysis createCatalysis(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Catalysis catalysis = new org.opencb.bionetdb.core.models.Catalysis();

        Catalysis catalysisBP = (Catalysis) bioPAXElement;

        // Common Interaction properties
        setInteractionCommonProperties(catalysisBP, catalysis);

        // Catalysis properties

        // controllers
        Set<Controller> controllers = catalysisBP.getController();
        for (Controller controller: controllers) {
            catalysis.getControllers().add(controller.getRDFId().split("#")[1]);
        }

        // controlled
        Set<Process> controlledProcesses = catalysisBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            catalysis.getControlledProcesses().add(controlledProcess.getRDFId().split("#")[1]);
        }

        // controlType
        catalysis.setControlType(catalysisBP.getControlType().toString());

        // cofactor
        Set<PhysicalEntity> cofactors = catalysisBP.getCofactor();
        for (PhysicalEntity cofactor: cofactors) {
            catalysis.getCofactors().add(cofactor.getRDFId().split("#")[1]);
        }

        return catalysis;
    }

    private org.opencb.bionetdb.core.models.Regulation createRegulation(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Regulation regulation = new org.opencb.bionetdb.core.models.Regulation();

        Control controlBP = (Control) bioPAXElement;

        // Common Interaction properties
        setInteractionCommonProperties(controlBP, regulation);

        // Regulation properties

        // controllers
        Set<Controller> controllers = controlBP.getController();
        for (Controller controller: controllers) {
            regulation.getControllers().add(controller.getRDFId().split("#")[1]);
        }

        // controlled
        Set<Process> controlledProcesses = controlBP.getControlled();
        for (Process controlledProcess: controlledProcesses) {
            regulation.getControlledProcesses().add(controlledProcess.getRDFId().split("#")[1]);
        }

        // controlType
        regulation.setControlType(controlBP.getControlType().toString());

        return regulation;
    }

    private void setInteractionCommonProperties(Interaction interactionBP,
                                                     org.opencb.bionetdb.core.models.Interaction interaction) {
        // = SPECIFIC PROPERTIES =

        // id
        interaction.setId(interactionBP.getRDFId().split("#")[1]);

        // description
        if (interactionBP.getComment() != null) {
            for (String comment : interactionBP.getComment()) {
                if (!comment.matches("(Authored:|Edited:|Reviewed:).+")) {
                    interaction.getDescription().add(comment);
                }
            }
        }

        // name
        if (interactionBP.getDisplayName() != null) {
            interaction.setName(interactionBP.getDisplayName());
        }

        // source
        List<String> sources = new ArrayList<>();
        Set<Provenance> provenances = interactionBP.getDataSource();
        for (Provenance provenance : provenances) {
            sources.addAll(provenance.getName());
        }
        interaction.setSource(sources);

        // participants
        Set<Entity> entities = interactionBP.getParticipant();
        for (Entity entity : entities) {
            interaction.getParticipants().add(entity.getRDFId().split("#")[1]);
        }

        // controlledBy
        Set<Control> controls = interactionBP.getControlledOf();
        for (Control control : controls) {
            interaction.getControlledBy().add(control.getRDFId().split("#")[1]);
        }

        // processOfPathway
        Set<PathwayStep> pathwaySteps = interactionBP.getStepProcessOf();
        for (PathwayStep pathwayStep : pathwaySteps) {
            interaction.getProcessOfPathway().add(pathwayStep.getPathwayOrderOf().getRDFId().split("#")[1]);
        }

        // xref
        Set<Xref> xrefs = interactionBP.getXref();
        for (Xref xref : xrefs) {
            if (xref.getDb() != null) {
                String source = xref.getDb().toLowerCase();
                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
                    Ontology ontology = new Ontology();
                    ontology.setSource(xref.getDb());
                    ontology.setSourceVersion(xref.getDbVersion());
                    ontology.setId(xref.getId());
                    ontology.setIdVersion(xref.getIdVersion());
                    interaction.setOntology(ontology);
                } else if (source.equals("pubmed")) {
                    Publication publication = new Publication();
                    publication.setSource(xref.getDb());
                    publication.setId(xref.getId());
                    interaction.setPublication(publication);
                } else {
                    org.opencb.bionetdb.core.models.Xref x = new org.opencb.bionetdb.core.models.Xref();
                    x.setSource(xref.getDb());
                    x.setSourceVersion(xref.getDbVersion());
                    x.setId(xref.getId());
                    x.setIdVersion(xref.getIdVersion());
                    interaction.setXref(x);
                }
            }
        }

        // publications
        for (Evidence evidence : interactionBP.getEvidence()) {
            for (Xref xref : evidence.getXref()) {
                PublicationXref pubXref = (PublicationXref) xref;
                Publication publication = new Publication();
                publication.setSource(pubXref.getDb());
                publication.setId(pubXref.getId());
                publication.setTitle(pubXref.getTitle());
                publication.setYear(pubXref.getYear());
                for (String author : pubXref.getAuthor()) {
                    publication.setAuthor(author);
                }
                for (String source : pubXref.getSource()) {
                    publication.setJournal(source);
                }
                interaction.setPublication(publication);
            }
        }

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

    }
}
