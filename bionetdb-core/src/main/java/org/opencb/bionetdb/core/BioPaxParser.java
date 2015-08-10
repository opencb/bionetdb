package org.opencb.bionetdb.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.biopax.paxtools.controller.EditorMap;
import org.biopax.paxtools.controller.PropertyEditor;
import org.biopax.paxtools.controller.SimpleEditorMap;
import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.Model;
import org.biopax.paxtools.model.level3.*;
import org.biopax.paxtools.model.level3.Process;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Protein;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 05/08/15.
 */
public class BioPaxParser {

    private String level;
    private EditorMap editorMap;

    private ObjectMapper objectMapper;

    private static final String REACTOME_PATHWAYS = "reactome.pathways";


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

    public void parse(Path path) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(path.toAbsolutePath().toString());
        BioPAXIOHandler handler = new SimpleIOHandler();
        Model model = handler.convertFromOWL(fileInputStream);

        Network network = new Network();

        // Selecting pathways
        Set<Pathway> pathways = model.getObjects(Pathway.class);

        for (Pathway pathway : pathways) {
            createPathway(pathway);
        }

        fileInputStream.close();

    }

    private void createPathway(Pathway pathway) {

        if(pathway.getRDFId().endsWith("Pathway842")) {

            Map<String, Collection> pathwayProperties = getBpeProperties(pathway);

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

        Map<String, Collection> processProperties = getBpeProperties(process);

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

        Map<String, Collection> moleculeProperties = getBpeProperties(molecule);

/*
        for (String prop : moleculeProperties.keySet()) {
            System.out.println("Molecule = " +  prop + ": " +
                    moleculeProperties.get(prop).toString());

        }
*/

    }

    private void createControl(Control control) {

        Map<String, Collection> controlProperties = getBpeProperties(control);

/*
        for (String prop : controlProperties.keySet()) {
            System.out.println("Control = " +  prop + ": " +
                    controlProperties.get(prop).toString());
        }
*/

        Set<Controller> controllers = control.getController();

        for (Controller controller : controllers) {
            Map<String, Collection> controllerProperties = getBpeProperties(controller);
        }

    }

    public Map<String, Collection> getBpeProperties(BioPAXElement bpe) {

        // Getting editors of the BioPAX element
        Set<PropertyEditor> editors = this.editorMap.getEditorsOf(bpe);

        // Creating table to store values
        Map<String, Collection> props = new HashMap<>();

        // Retrieving properties and their values
        int row = 0;
        for (PropertyEditor editor : editors) {
            // First column is property and second column is value
            props.put(editor.getProperty(), editor.getValueFromBean(bpe));
            row++;
        }

        return props;

    }


    private Protein createProtein(BioPAXElement bpe) {
        Protein protein = new Protein("", "");


        return protein;
    }

}
