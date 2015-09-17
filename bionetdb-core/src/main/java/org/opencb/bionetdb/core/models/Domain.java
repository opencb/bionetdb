package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dapregi on 16/09/15.
 */
public class Domain {

    protected String name;
    protected String description;
    protected List<Xref> xrefs;
    protected List<Ontology> ontologies;

    public Domain() {
        init();
    }
    
    private void init() {
        this.name = "";
        this.description = "";
        this.ontologies = new ArrayList<>();
        this.xrefs = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Xref> getXrefs() {
        return xrefs;
    }

    public void setXrefs(List<Xref> xrefs) {
        this.xrefs = xrefs;
    }

    public void setXref(Xref xref) {
        // Adding xref unless it exists
        boolean duplicate = false;
        for (Xref currentXref : this.getXrefs()) {
            if(xref.getSource().equals(currentXref.getSource()) &&
                    xref.getSourceVersion().equals(currentXref.getSourceVersion()) &&
                    xref.getId().equals(currentXref.getId()) &&
                    xref.getIdVersion().equals(currentXref.getIdVersion())) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getXrefs().add(xref);
        }
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
            if(ontology.getSource().equals(currentOntology.getSource()) &&
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
