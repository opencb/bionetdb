package org.opencb.bionetdb.core.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.biopax.paxtools.controller.EditorMap;
import org.biopax.paxtools.controller.PropertyEditor;
import org.biopax.paxtools.controller.SimpleEditorMap;
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
    private EditorMap editorMap;

    private ObjectMapper objectMapper;

    private static final String REACTOME_FEAT = "reactome.";


    public BioPaxParser(String level) {
        this.level = level;

        init();
    }

    private void init() {

        switch (level.toLowerCase()) {
            case "l1":
                editorMap = SimpleEditorMap.L1;
                break;
            case "l2":
                editorMap = SimpleEditorMap.L2;
                break;
            case "l3":
            default:
                editorMap = SimpleEditorMap.L3;
                break;
        }
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
                case "Dna":
                    network.getPhysicalEntities().add(createDna(bioPAXElement));
                    break;
                case "Rna":
                    network.getPhysicalEntities().add(createRna(bioPAXElement));
                    break;
                case "Protein":
                    network.getPhysicalEntities().add(createProtein(bioPAXElement));
                    break;
                case "Complex":
                    network.getPhysicalEntities().add(createComplex(bioPAXElement));
                    break;
                case "SmallMolecule":
                    network.getPhysicalEntities().add(createSmallMolecule(bioPAXElement));
                    break;

                // Interactions
                case "BiochemicalReaction":
                case "TemplateReaction":
                case "Degradation":
                    network.getInteractions().add(createReaction(bioPAXElement));
                    break;
                case "Catalysis":
                    network.getInteractions().add(createCatalysis(bioPAXElement));
                    break;
                case "Modulation":
                case "TemplateReactionRegulation":
                    network.getInteractions().add(createRegulation(bioPAXElement));
                    break;
                case "ComplexAssembly":
                case "MolecularInteraction":
                    network.getInteractions().add(createAssembly(bioPAXElement));
                    break;
                case "Transport":
                case "TransportWithBiochemicalReaction":
                    network.getInteractions().add(createTransport(bioPAXElement));
                    break;
            }

        }
        inputStream.close();
        return network;
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
            List<String> altIds = new ArrayList<>();
            altIds.addAll(entityReference.getName());
            dna.setAltIds(altIds);

            // description
            dna.setDescription(entityReference.getComment().toString());
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
            List<String> altIds = new ArrayList<>();
            altIds.addAll(entityReference.getName());
            rna.setAltIds(altIds);

            // description
            rna.setDescription(entityReference.getComment().toString());
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
            List<String> altIds = new ArrayList<>();
            altIds.addAll(entityReference.getName());
            protein.setAltIds(altIds);

            // description
            protein.setDescription(entityReference.getComment().toString());

            // uniProtId
            Set<Xref> xrefs = entityReference.getXref();
            for (Xref xref : xrefs) {
                protein.setUniProtId(xref.getId());
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
            List<String> altIds = new ArrayList<>();
            altIds.addAll(entityReference.getName());
            smallMolecule.setAltIds(altIds);

            // description
            smallMolecule.setDescription(entityReference.getComment().toString());
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

    private void setPhysicalEntityCommonProperties (PhysicalEntity physicalEntityBP,
                                      org.opencb.bionetdb.core.models.PhysicalEntity physicalEntity) {
        // = SPECIFIC PROPERTIES =

        // id
        physicalEntity.setId(physicalEntityBP.getRDFId().split("#")[1]);

        // name
        physicalEntity.setName(physicalEntityBP.getDisplayName());

        // altNames
        List<String> altNames = new ArrayList<>();
        altNames.addAll(physicalEntityBP.getName());
        physicalEntity.setAltNames(altNames);

        // cellularLocation.names
        List<String> cellularLocationsNames = new ArrayList<>();
        cellularLocationsNames.addAll(physicalEntityBP.getCellularLocation().getTerm());

        // cellularLocation.ids
        List<String> cellularLocationsIds = new ArrayList<>();
        Set<Xref> CellLocXrefs = physicalEntityBP.getCellularLocation().getXref();
        for (Xref CellLocXref : CellLocXrefs) {
            cellularLocationsIds.add(CellLocXref.getId());
        }

        // cellularLocation
        Map<String, List<String>> cellularLocations = new HashMap<>();
        cellularLocations.put("name", cellularLocationsNames);
        cellularLocations.put("id", cellularLocationsIds);
        physicalEntity.setCellularLocation(cellularLocations);

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

        //memberOf
        Set<PhysicalEntity> pesOf = physicalEntityBP.getMemberPhysicalEntityOf();
        for (PhysicalEntity peOf : pesOf) {
            physicalEntity.getMemberOfSet().add(peOf.getRDFId().split("#")[1]);
        }

        //componentOf
        Set<Complex> complexes = physicalEntityBP.getComponentOf();
        for (Complex complex : complexes) {
            physicalEntity.getComponentOfComplex().add(complex.getRDFId().split("#")[1]);
        }

        //participantOf
        Set<Interaction> interactions = physicalEntityBP.getParticipantOf();
        for (Interaction interaction : interactions) {
            physicalEntity.getParticipantOfInteraction().add(interaction.getRDFId().split("#")[1]);
        }

        // = NONSPECIFIC PROPERTIES =

        // xref
        List<Map<String, String>> xreferences = new ArrayList<>();
        Set<Xref> xrefs = physicalEntityBP.getXref();
        for (Xref xref : xrefs) {
            Map<String, String> db = new HashMap<>();
            db.put("db", xref.getDb());
            db.put("dbVersion", xref.getDbVersion());
            db.put("id", xref.getId());
            db.put("idVersion", xref.getIdVersion());
            xreferences.add(db);
        }
        physicalEntity.getAttributes().put(REACTOME_FEAT + "xref", xreferences);

        // comment
        physicalEntity.getAttributes().put(REACTOME_FEAT + "comment",
                physicalEntityBP.getComment().toString());

        // availability
        physicalEntity.getAttributes().put(REACTOME_FEAT + "availability",
                physicalEntityBP.getAvailability().toString());

        // evidence
        physicalEntity.getAttributes().put(REACTOME_FEAT + "evidence",
                physicalEntityBP.getEvidence().toString());

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

        switch (bioPAXElement.getModelInterface().getSimpleName()) {
            case "TemplateReaction":
                TemplateReaction templateReactBP = (TemplateReaction) bioPAXElement;

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
                Conversion conversionBP = (Conversion) bioPAXElement;

                // Common Interaction properties
                setInteractionCommonProperties(conversionBP, reaction);

                // Degradation properties

                // Left items
                List<String> leftItems = new ArrayList<String>();
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

    private org.opencb.bionetdb.core.models.Assembly createAssembly(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Assembly assembly = new org.opencb.bionetdb.core.models.Assembly();

        switch (bioPAXElement.getModelInterface().getSimpleName()) {
            case "MolecularInteraction":
                MolecularInteraction molecularInteractionBP = (MolecularInteraction) bioPAXElement;

                // Common Interaction properties
                setInteractionCommonProperties(molecularInteractionBP, assembly);
                break;
            case "ComplexAssembly":
                ComplexAssembly complexAssemblyBP = (ComplexAssembly) bioPAXElement;

                // Common Interaction properties
                setInteractionCommonProperties(complexAssemblyBP, assembly);

                // ComplexAssembly properties

                // Left items
                List<String> leftItems = new ArrayList<String>();
                Set<PhysicalEntity> lefts = complexAssemblyBP.getLeft();
                for (PhysicalEntity left : lefts) {
                    leftItems.add(left.getRDFId().split("#")[1]);
                }

                // Right items
                List<String> rightItems = new ArrayList<>();
                Set<PhysicalEntity> rights = complexAssemblyBP.getRight();
                for (PhysicalEntity right : rights) {
                    rightItems.add(right.getRDFId().split("#")[1]);
                }

                if (complexAssemblyBP.getConversionDirection() != null) {
                    switch (complexAssemblyBP.getConversionDirection().toString()) {
                        case "REVERSIBLE":
                            assembly.setReversible(true);
                            // NO BREAK HERE
                        case "LEFT-TO-RIGHT":
                        case "LEFT_TO_RIGHT":
                            assembly.setReactants(leftItems);
                            assembly.setProducts(rightItems);
                            break;
                        case "RIGHT-TO-LEFT":
                        case "RIGHT_TO_LEFT":
                            assembly.setReactants(rightItems);
                            assembly.setProducts(leftItems);
                            break;
                    }
                } else {
                    assembly.setReactants(leftItems);
                    assembly.setProducts(rightItems);
                }

                // Spontaneous
                if (complexAssemblyBP.getSpontaneous() != null) {
                    if (complexAssemblyBP.getSpontaneous()) {
                        assembly.setSpontaneous(true);
                    } else {
                        assembly.setSpontaneous(false);
                    }
                }

                // Stoichiometry
                List<Map<String, Object>> stoichiometry = new ArrayList<>();
                Set<Stoichiometry> stoichiometryItems = complexAssemblyBP.getParticipantStoichiometry();
                for (Stoichiometry stoichiometryItem : stoichiometryItems) {
                    Map<String, Object> stchmtr = new HashMap<>();
                    stchmtr.put("component", stoichiometryItem.getPhysicalEntity().toString().split("#")[1]);
                    stchmtr.put("coefficient", stoichiometryItem.getStoichiometricCoefficient());
                    stoichiometry.add(stchmtr);
                }
                assembly.setStoichiometry(stoichiometry);
                break;
        }
        return assembly;
    }

    private org.opencb.bionetdb.core.models.Transport createTransport(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Transport transport = new org.opencb.bionetdb.core.models.Transport();

        Conversion conversionBP = (Conversion) bioPAXElement;

        // Common Interaction properties
        setInteractionCommonProperties(conversionBP, transport);

        // Transport properties

        // Left items
        List<String> leftItems = new ArrayList<String>();
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
                    transport.setReversible(true);
                    // NO BREAK HERE
                case "LEFT-TO-RIGHT":
                case "LEFT_TO_RIGHT":
                    transport.setReactants(leftItems);
                    transport.setProducts(rightItems);
                    break;
                case "RIGHT-TO-LEFT":
                case "RIGHT_TO_LEFT":
                    transport.setReactants(rightItems);
                    transport.setProducts(leftItems);
                    break;
            }
        } else {
            transport.setReactants(leftItems);
            transport.setProducts(rightItems);
        }

        // Spontaneous
        if (conversionBP.getSpontaneous() != null) {
            if (conversionBP.getSpontaneous()) {
                transport.setSpontaneous(true);
            } else {
                transport.setSpontaneous(false);
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
        transport.setStoichiometry(stoichiometry);

        return transport;
    }

    private void setInteractionCommonProperties (Interaction interactionBP,
                                                     org.opencb.bionetdb.core.models.Interaction interaction) {
        // = SPECIFIC PROPERTIES =

        // id
        interaction.setId(interactionBP.getRDFId().split("#")[1]);

        // name
        interaction.setName(interactionBP.getDisplayName());

        // source
        List<String> sources = new ArrayList<>();
        Set<Provenance> provenances = interactionBP.getDataSource();
        for (Provenance provenance : provenances) {
            interaction.getSource().addAll(provenance.getName());
        }
        interaction.setSource(sources);

        // participants
        Set<Entity> entities = interactionBP.getParticipant();
        for (Entity entity : entities) {
            interaction.getParticipants().add(entity.getRDFId().split("#")[1]);
        }

        // participantOf
        Set<Interaction> participantOfInters = interactionBP.getParticipantOf();
        for (Interaction participantOfInter : participantOfInters) {
            interaction.getParticipantOf().add(participantOfInter.getRDFId().split("#")[1]);
        }

        // controlledBy
        Set<Control> controls = interactionBP.getControlledOf();
        for (Control control : controls) {
            interaction.getControlledBy().add(control.getRDFId().split("#")[1]);
        }

        // pathwayComponentOf
        Set<Pathway> pathways = interactionBP.getPathwayComponentOf();
        for (Pathway pathway : pathways) {
            interaction.getPathwayComponentOf().add(pathway.getRDFId().split("#")[1]);
        }

        // processOf
        Set<PathwayStep> pathwaySteps = interactionBP.getStepProcessOf();
        for (PathwayStep pathwayStep : pathwaySteps) {
            interaction.getProcessOf().add(pathwayStep.getPathwayOrderOf().getRDFId().split("#")[1]);
        }

        // = NONSPECIFIC PROPERTIES =

        // xref
        List<Map<String, String>> xreferences = new ArrayList<>();
        Set<Xref> xrefs = interactionBP.getXref();
        for (Xref xref : xrefs) {
            Map<String, String> db = new HashMap<>();
            db.put("db", xref.getDb());
            db.put("dbVersion", xref.getDbVersion());
            db.put("id", xref.getId());
            db.put("idVersion", xref.getIdVersion());
            xreferences.add(db);
        }
        interaction.getAttributes().put(REACTOME_FEAT + "xref", xreferences);

        // comment
        interaction.getAttributes().put(REACTOME_FEAT + "comment",
                interactionBP.getComment().toString());

        // availability
        interaction.getAttributes().put(REACTOME_FEAT + "availability",
                interactionBP.getAvailability().toString());

        // evidence
        interaction.getAttributes().put(REACTOME_FEAT + "evidence",
                interactionBP.getEvidence().toString());

        // annotations
        interaction.getAttributes().put(REACTOME_FEAT + "annotations",
                interactionBP.getAnnotations());

        // annotations
        interaction.getAttributes().put(REACTOME_FEAT + "interactionType",
                interactionBP.getInteractionType().toString());

        // annotations
        interaction.getAttributes().put(REACTOME_FEAT + "name",
                interactionBP.getName().toString());

    }

    // TODO extract pathway hierarchy

    @Deprecated
    public Map<String, List> getBpeProperties(BioPAXElement bpe) {
        // Getting editors of the BioPAX element
        Set<PropertyEditor> editors = this.editorMap.getEditorsOf(bpe);

        // Creating table to store values
        Map<String, List> props = new HashMap<>();

        // Retrieving properties and their values
        for (PropertyEditor editor : editors) {
            // First column is property and second column is value
            props.put(editor.getProperty(), new ArrayList<>(editor.getValueFromBean(bpe)));
        }
        return props;
    }
}
