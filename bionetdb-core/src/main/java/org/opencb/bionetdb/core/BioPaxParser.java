package org.opencb.bionetdb.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.BioPAXFactory;
import org.biopax.paxtools.model.BioPAXLevel;
import org.biopax.paxtools.model.Model;
import org.biopax.paxtools.model.level3.Protein;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by imedina on 05/08/15.
 */
public class BioPaxParser {

    private String level;
    private BioPAXFactory factory;

    private ObjectMapper objectMapper;

    public BioPaxParser(String level) {
        this.level = level;

        objectMapper = new ObjectMapper();

        init();
    }

    private void init() {
        switch (level.toLowerCase()) {
            case "l1":
                factory = BioPAXLevel.L1.getDefaultFactory();
                break;
            case "l2":
                factory = BioPAXLevel.L2.getDefaultFactory();
                break;
            case "l3":
            default:
                factory = BioPAXLevel.L3.getDefaultFactory();
                break;
        }
    }


    public void parse(Path path) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(path.toAbsolutePath().toString());
        BioPAXIOHandler handler = new SimpleIOHandler();
        Model model = handler.convertFromOWL(fileInputStream);

        Set<BioPAXElement> objects = model.getObjects();
        Iterator<BioPAXElement> iterator = objects.iterator();

        while (iterator.hasNext()) {
            BioPAXElement bioPAXElement = iterator.next();

            switch (bioPAXElement.getModelInterface().getSimpleName()) {
                case "Protein":
                    createProteinElement(bioPAXElement);
                    break;
            }
        }
        fileInputStream.close();
    }

    private void createProteinElement(BioPAXElement bioPAXElement) throws JsonProcessingException {
        if(bioPAXElement.getRDFId().endsWith("Protein1")) {
            Protein protein = (Protein)bioPAXElement;
            System.out.println("next = " + protein.getModelInterface().getSimpleName());
            System.out.println("next = " + protein.getCellularLocation());
            System.out.println("next = " + protein.getDisplayName());
        }
    }

}
