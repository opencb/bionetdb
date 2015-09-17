package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dapregi on 3/09/15.
 */
public class CellularLocation {

    private List<String> names;
    private List<Ontology> ontologies;

    public CellularLocation() {
        this.names = new ArrayList<>();
        this.ontologies = new ArrayList<>();
    }

    public CellularLocation(List<String> names, List<Ontology> ontologies) {
        this.names = names;
        this.ontologies = ontologies;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

    public List<Ontology> getOntologies() {
        return ontologies;
    }

    public void setOntologies(List<Ontology> ontologies) {
        this.ontologies = ontologies;
    }

    public void setOntology(Ontology ontology) {
        // Adding ontology unless it exists
        boolean duplicate = false;
        for (Ontology currentOntology : this.getOntologies()) {
            if (ontology.getSource().equals(currentOntology.getSource()) &&
                    ontology.getSourceVersion().equals(currentOntology.getSourceVersion()) &&
                    ontology.getId().equals(currentOntology.getId()) &&
                    ontology.getIdVersion().equals(currentOntology.getIdVersion())) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getOntologies().add(ontology);
        }
    }
}
