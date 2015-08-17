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
import org.opencb.bionetdb.core.models.Network;

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
        }else {
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

        // Getting BioPAX element properties
        Map<String, List> properties = getBpeProperties(bioPAXElement);

        // Setting id
        dna.setId(bioPAXElement.getRDFId().split("#")[1]);

        // Retrieving attributes values
        for (Map.Entry<String, List> entry : properties.entrySet()) {
            switch (entry.getKey()) {
                case "displayName":
                    dna.setName(entry.getValue().toString());
                    break;
                case "name":
                    dna.setAltNames(entry.getValue());
                    break;
                case "cellularLocation":
                    List<CellularLocationVocabulary> cellularLocationVocabularies = entry.getValue();
                    List<String> cellularLocations = new ArrayList<>();
                    for (CellularLocationVocabulary cellularLocationVocabulary : cellularLocationVocabularies) {
                        cellularLocations.addAll(cellularLocationVocabulary.getTerm());
                    }
                    dna.setCellularLocation(cellularLocations);
                    break;
                case "dataSource":
                    dna.setSource(entry.getValue());
                    break;
                case "entityReference":
                    if (!entry.getValue().isEmpty()) {
                        // TODO Get organism and NCBI taxonomy from "entityReference"
                        Map<String, List> propertiesER;
                        propertiesER = getBpeProperties(dnaBP.getEntityReference());
                        dna.setAltIds(propertiesER.get("name"));
                    }
                    break;
                default:
                    // Nonspecific attributes
                    dna.getAttributes().put(REACTOME_FEAT + entry.getKey(), entry.getValue());
                    break;
            }
        }
        return dna;
    }

    private org.opencb.bionetdb.core.models.Rna createRna(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Rna rna = new org.opencb.bionetdb.core.models.Rna();

        Rna rnaBP = (Rna) bioPAXElement;

        // Getting BioPAX element properties
        Map<String, List> properties = getBpeProperties(bioPAXElement);

        // Setting id
        rna.setId(bioPAXElement.getRDFId().split("#")[1]);

        // Retrieving attributes values
        for (Map.Entry<String, List> entry : properties.entrySet()) {

            switch (entry.getKey()) {
                case "displayName":
                    rna.setName(entry.getValue().toString());
                case "name":
                    rna.setAltNames(entry.getValue());
                    break;
                case "cellularLocation":
                    List<CellularLocationVocabulary> cellularLocationVocabularies = entry.getValue();
                    List<String> cellularLocations = new ArrayList<>();
                    for (CellularLocationVocabulary cellularLocationVocabulary : cellularLocationVocabularies) {
                        cellularLocations.addAll(cellularLocationVocabulary.getTerm());
                    }
                    rna.setCellularLocation(cellularLocations);
                    break;
                case "dataSource":
                    rna.setSource(entry.getValue());
                    break;
                case "entityReference":
                    if (!entry.getValue().isEmpty()) {
                        // TODO Get organism and NCBI taxonomy from "entityReference"
                        Map<String, List> propertiesER;
                        propertiesER = getBpeProperties(rnaBP.getEntityReference());
                        rna.setAltIds(propertiesER.get("name"));
                    }
                    break;
                default:
                    // Nonspecific attributes
                    rna.getAttributes().put(REACTOME_FEAT + entry.getKey(), entry.getValue());
                    break;
            }
        }
        return rna;
    }

    private org.opencb.bionetdb.core.models.Protein createProtein(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Protein protein = new org.opencb.bionetdb.core.models.Protein();
        Protein proteinBP = (Protein) bioPAXElement;

        // Getting BioPAX element properties
        Map<String, List> properties = getBpeProperties(bioPAXElement);

        // Setting id
        protein.setId(bioPAXElement.getRDFId().split("#")[1]);

        // Retrieving attributes values
        for (Map.Entry<String, List> entry : properties.entrySet()) {

            switch (entry.getKey()) {
                case "displayName":
                    protein.setName(entry.getValue().toString());
                    break;
                case "name":
                    protein.setAltNames(entry.getValue());
                    break;
                case "cellularLocation":
                    List<CellularLocationVocabulary> cellularLocationVocabularies = entry.getValue();
                    List<String> cellularLocations = new ArrayList<>();
                    for (CellularLocationVocabulary cellularLocationVocabulary : cellularLocationVocabularies) {
                        cellularLocations.addAll(cellularLocationVocabulary.getTerm());
                    }
                    protein.setCellularLocation(cellularLocations);
                    break;
                case "dataSource":
                    protein.setSource(entry.getValue());
                    break;
                case "entityReference":
                    if (!entry.getValue().isEmpty()) {
                        // TODO Get organism and NCBI taxonomy from "entityReference"
                        Map<String, List> propertiesER;
                        propertiesER = getBpeProperties(proteinBP.getEntityReference());
                        protein.setAltIds(propertiesER.get("name"));
                        protein.setUniProtId(propertiesER.get("xref").toString());
                        protein.setDescription(propertiesER.get("comment").toString());
                    }
                    break;
                default:
                    // Nonspecific attributes
                    protein.getAttributes().put(REACTOME_FEAT + entry.getKey(), entry.getValue());
                    break;

            }
        }
        return protein;
    }


    private org.opencb.bionetdb.core.models.Complex createComplex(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.Complex complex;
        complex = new org.opencb.bionetdb.core.models.Complex();

        Complex complexBP = (Complex) bioPAXElement;

        // Getting BioPAX element properties
        Map<String, List> properties = getBpeProperties(bioPAXElement);

        // Setting id
        complex.setId(bioPAXElement.getRDFId().split("#")[1]);

        // Retrieving attributes values
        for (Map.Entry<String, List> entry : properties.entrySet()) {

            switch (entry.getKey()) {
                case "displayName":
                    complex.setName(entry.getValue().toString());
                    break;
                case "name":
                    complex.setAltNames(entry.getValue());
                    break;
                case "cellularLocation":
                    List<CellularLocationVocabulary> cellularLocationVocabularies = entry.getValue();
                    List<String> cellularLocations = new ArrayList<>();
                    for (CellularLocationVocabulary cellularLocationVocabulary : cellularLocationVocabularies) {
                        cellularLocations.addAll(cellularLocationVocabulary.getTerm());
                    }
                    complex.setCellularLocation(cellularLocations);
                    break;
                case "dataSource":
                    complex.setSource(entry.getValue());
                    break;
                case "component":
                    ArrayList<String> components = new ArrayList<>();
                    for (PhysicalEntity component : complexBP.getComponent()) {
                        components.add(component.getRDFId().split("#")[1]);
                    }
                    complex.setComponents(components);
                    break;
                case "componentStoichiometry":
                    Map<String, Float> stoichiometries = new HashMap<>();
                    for (Stoichiometry stoichiometry : complexBP.getComponentStoichiometry()) {
                        stoichiometries.put(
                                stoichiometry.getPhysicalEntity().toString().split("#")[1],
                                stoichiometry.getStoichiometricCoefficient());
                    }
                    complex.setStoichiometry(stoichiometries);
                    break;
                default:
                    // Nonspecific attributes
                    complex.getAttributes().put(REACTOME_FEAT + entry.getKey(), entry.getValue());
                    break;
            }
        }
        return complex;
    }


    private org.opencb.bionetdb.core.models.SmallMolecule createSmallMolecule(BioPAXElement bioPAXElement) {
        org.opencb.bionetdb.core.models.SmallMolecule smallMolecule;
        smallMolecule = new org.opencb.bionetdb.core.models.SmallMolecule();

        SmallMolecule smallMoleculeBP = (SmallMolecule) bioPAXElement;

        // Getting BioPAX element properties
        Map<String, List> properties = getBpeProperties(bioPAXElement);

        // Setting id
        smallMolecule.setId(bioPAXElement.getRDFId().split("#")[1]);

        // Retrieving attributes values
        for (Map.Entry<String, List> entry : properties.entrySet()) {

            switch (entry.getKey()) {
                case "displayName":
                    smallMolecule.setName(entry.getValue().toString());
                    break;
                case "name":
                    smallMolecule.setAltNames(entry.getValue());
                    break;
                case "cellularLocation":
                    List<CellularLocationVocabulary> cellularLocationVocabularies = entry.getValue();
                    List<String> cellularLocations = new ArrayList<>();
                    for (CellularLocationVocabulary cellularLocationVocabulary : cellularLocationVocabularies) {
                        cellularLocations.addAll(cellularLocationVocabulary.getTerm());
                    }
                    smallMolecule.setCellularLocation(cellularLocations);
                    break;
                case "dataSource":
                    smallMolecule.setSource(entry.getValue());
                    break;
                case "entityReference":
                    if (!entry.getValue().isEmpty()) {
                        // TODO Get organism and NCBI taxonomy from "entityReference"
                        Map<String, List> propertiesER;
                        propertiesER = getBpeProperties(smallMoleculeBP.getEntityReference());
                        smallMolecule.setAltIds(propertiesER.get("name"));
                    }
                    break;
                default:
                    // Nonspecific attributes
                    smallMolecule.getAttributes().put(REACTOME_FEAT + entry.getKey(),
                            entry.getValue());
                    break;
            }
        }
        return smallMolecule;
    }


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

    @Deprecated
    private void setCommonProperties (org.opencb.bionetdb.core.models.PhysicalEntity physicalEntity, Map<String, List> properties){
        physicalEntity.setName(properties.get("displayName").toString());
        physicalEntity.setAltNames(properties.get("name"));
        physicalEntity.setCellularLocation(properties.get("cellularLocation"));
        physicalEntity.setSource(properties.get("dataSource"));
    }

}
