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
import org.biopax.paxtools.model.level3.Complex;
import org.biopax.paxtools.model.level3.Dna;
import org.biopax.paxtools.model.level3.Interaction;
import org.biopax.paxtools.model.level3.PhysicalEntity;
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
                    //createReaction(bioPAXElement, properties);
                    break;
                case "Catalysis":
                    //createCatalysis(bioPAXElement, properties);
                    break;
                case "Modulation":
                case "TemplateReactionRegulation":
                    //createRegulation(bioPAXElement, properties);
                    break;
                case "ComplexAssembly":
                case "MolecularInteraction":
                    //createAssembly(bioPAXElement, properties);
                    break;
                case "Transport":
                case "TransportWithBiochemicalReaction":
                    //createTransport(bioPAXElement, properties);
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
        setCommonProperties(dnaBP, dna);

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
        setCommonProperties(rnaBP, rna);

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
        setCommonProperties(proteinBP, protein);

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
        setCommonProperties(smallMoleculeBP, smallMolecule);

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
        setCommonProperties(complexBP, complex);

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

    private void setCommonProperties (PhysicalEntity physicalEntityBP,
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
        List<String> members = new ArrayList<>();
        Set<PhysicalEntity> pes = physicalEntityBP.getMemberPhysicalEntity();
        for (PhysicalEntity pe : pes) {
            members.add(pe.getRDFId().split("#")[1]);
        }
        physicalEntity.setMembers(members);

        //memberOf
        List<String> membersOf = new ArrayList<>();
        Set<PhysicalEntity> pesOf = physicalEntityBP.getMemberPhysicalEntityOf();
        for (PhysicalEntity peOf : pesOf) {
            membersOf.add(peOf.getRDFId().split("#")[1]);
        }
        physicalEntity.setMemberOfSet(membersOf);

        //componentOf
        List<String> componentOf = new ArrayList<>();
        Set<Complex> complexes = physicalEntityBP.getComponentOf();
        for (Complex complex : complexes) {
            componentOf.add(complex.getRDFId().split("#")[1]);
        }
        physicalEntity.setComponentOfComplex(componentOf);

        //participantOf
        List<String> participantOf = new ArrayList<>();
        Set<Interaction> interactions = physicalEntityBP.getParticipantOf();
        for (Interaction interaction : interactions) {
            participantOf.add(interaction.getRDFId().split("#")[1]);
        }
        physicalEntity.setParticipantOfInteraction(participantOf);

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
