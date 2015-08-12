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
import org.biopax.paxtools.model.level3.Dna;
import org.biopax.paxtools.model.level3.PhysicalEntity;
import org.biopax.paxtools.model.level3.Process;
import org.opencb.bionetdb.core.models.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

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

        // Retrieving model from BioPAX file
        FileInputStream fileInputStream = new FileInputStream(path.toAbsolutePath().toString());
        BioPAXIOHandler handler = new SimpleIOHandler();
        Model model = handler.convertFromOWL(fileInputStream);

        // Retrieving BioPAX elements
        Set<BioPAXElement> bioPAXElements = model.getObjects();

        for (BioPAXElement bioPAXElement : bioPAXElements) {

            // Getting BioPAX element properties
            Map<String, List> properties = getBpeProperties(bioPAXElement);

            switch (bioPAXElement.getModelInterface().getSimpleName()) {

                // Physical Entities
                case "Dna":
                    createDna(bioPAXElement, properties);
                    break;
                case "Rna":
                    //createRna(bioPAXElement, properties);
                    break;
                case "Protein":
                    //createProtein(bioPAXElement, properties);
                    break;
                case "Complex":
                    //createComplex(bioPAXElement, properties);
                    break;
                case "SmallMolecule":
                    //createSmallMolecule(bioPAXElement, properties);
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

        fileInputStream.close();

        return network;
    }

    private void createDna(BioPAXElement bioPAXElement, Map<String, List> properties) {

        org.opencb.bionetdb.core.models.Dna dna = new org.opencb.bionetdb.core.models.Dna();

        Dna dnaBP = (Dna) bioPAXElement;

        // Setting id
        dna.setId(bioPAXElement.getRDFId().split("#")[1]);

        // Retrieving attributes values
        for (Map.Entry<String, List> entry : properties.entrySet()) {

            switch (entry.getKey()) {
                case "name":
                    System.out.println(entry.getValue());
                    dna.setName((String) entry.getValue().iterator().next());
                    break;
                case "cellularLocation":
                    dna.setCellularLocation(entry.getValue());
                    break;
                case "dataSource":
                    dna.setSource(entry.getValue());
                    break;
                case "entityReference":
                    if (!entry.getValue().isEmpty()) {
                        // todo Get organism and NCBI taxonomy from "entityReference"
                        Map<String, List> propertiesER = getBpeProperties(dnaBP.getEntityReference());
                        dna.setAltNames(propertiesER.get("name"));
                    }
                    break;
                default:
                    // Nonspecific attributes
                    dna.getAttributes().put(REACTOME_FEAT + entry.getKey(), entry.getValue());
                    break;
            }
        }

        // Retrieving processes where the molecule is a participant.
        ArrayList<String> processes = new ArrayList<>();

        for (BioPAXElement process : dnaBP.getParticipantOf()) {
            processes.add(process.toString().split("#")[1]);
        }

            dna.setParticipantOf(processes);

    }


    private void createPathway(Pathway pathway) {

        if(pathway.getRDFId().endsWith("Pathway842")) {

            Map<String, List> pathwayProperties = getBpeProperties(pathway);

/*
            for (String prop : pathwayProperties.keySet()) {
                System.out.println("Pathway = " + prop + ": " +
                        pathwayProperties.get(prop).toString());
            }
*/

            Set<Process> processes = pathway.getPathwayComponent();

            for (Process process : processes) {
                createProcess(process);
            }

        }

    }

    private void createProcess(Process process) {

        Map<String, List> processProperties = getBpeProperties(process);

/*
        for (String prop : processProperties.keySet()) {
            System.out.println("Process = " +  prop + ": " +
                    processProperties.get(prop).toString());
        }
*/
        String [] componentTypes = {"left", "right"};

        for (String componentType : componentTypes) {
            Collection molecules = processProperties.get(componentType);

            for (Object molecule : molecules) {
                createMolecule((PhysicalEntity) molecule);
            }

        }

        Set<Control> controls = process.getControlledOf();

        for (Control control : controls) {
            createControl(control);

        }

    }

    private void createMolecule(PhysicalEntity molecule) {

        Map<String, List> moleculeProperties = getBpeProperties(molecule);

/*
        for (String prop : moleculeProperties.keySet()) {
            System.out.println("Molecule = " +  prop + ": " +
                    moleculeProperties.get(prop).toString());

        }
*/

    }

    private void createControl(Control control) {

        Map<String, List> controlProperties = getBpeProperties(control);

/*
        for (String prop : controlProperties.keySet()) {
            System.out.println("Control = " +  prop + ": " +
                    controlProperties.get(prop).toString());
        }
*/

        Set<Controller> controllers = control.getController();

        for (Controller controller : controllers) {
            Map<String, List> controllerProperties = getBpeProperties(controller);
        }

    }

    public Map<String, List> getBpeProperties(BioPAXElement bpe) {

        // Getting editors of the BioPAX element
        Set<PropertyEditor> editors = this.editorMap.getEditorsOf(bpe);

        // Creating table to store values
        Map<String, List> props = new HashMap<>();

        // Retrieving properties and their values
        int row = 0;
        for (PropertyEditor editor : editors) {
            // First column is property and second column is value
            props.put(editor.getProperty(), new ArrayList<>(editor.getValueFromBean(bpe)));
            row++;
        }

        return props;

    }


/*
    private Protein createProtein(BioPAXElement bpe) {
        Protein protein = new Protein("", "");


        return protein;
    }
*/

}
