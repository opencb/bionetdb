package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by imedina on 10/08/15.
 */
public class Interaction {

    protected String id;
    protected String name;
    protected List<String> description;
    protected List<String> source;
    protected List<String> participants;
    protected List<String> processOfPathway;
    protected List<String> controlledBy;
    protected List<Xref> xrefs;
    protected List<Ontology> ontologies;
    protected List<Publication> publications;

    protected Type type;

    protected Map<String, Object> attributes;

    public enum Type {
        REACTION       ("reaction"),
        CATALYSIS      ("catalysis"),
        REGULATION     ("regulation"),
        COLOCALIZATION ("colocalization");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public Interaction() {
        this.description = new ArrayList<>();
        this.id = "";
        this.name = "";

        // init rest of attributes
        init();
    }

    public Interaction(String id, String name, List<String> description, Type type) {
        this.description = description;
        this.id = id;
        this.name = name;
        this.type = type;

        // init rest of attributes
        init();
    }

    private void init() {
        this.attributes = new HashMap<>();
        this.source = new ArrayList<>();
        this.participants = new ArrayList<>();
        this.processOfPathway = new ArrayList<>();
        this.controlledBy = new ArrayList<>();
        this.xrefs = new ArrayList<>();
        this.ontologies = new ArrayList<>();
        this.publications = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getDescription() {
        return description;
    }

    public void setDescription(List<String> description) {
        this.description = description;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public List<String> getSource() {
        return source;
    }

    public void setSource(List<String> source) {
        this.source = source;
    }

    public List<String> getParticipants() {
        return participants;
    }

    public void setParticipants(List<String> participants) {
        this.participants = participants;
    }

    public List<String> getProcessOfPathway() {
        return processOfPathway;
    }

    public void setProcessOfPathway(List<String> processOfPathway) {
        this.processOfPathway = processOfPathway;
    }

    public List<String> getControlledBy() {
        return controlledBy;
    }

    public void setControlledBy(List<String> controlledOf) {
        this.controlledBy = controlledOf;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
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
            if (xref.getSource().equals(currentXref.getSource())
                    && xref.getSourceVersion().equals(currentXref.getSourceVersion())
                    && xref.getId().equals(currentXref.getId())
                    && xref.getIdVersion().equals(currentXref.getIdVersion())) {
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
            if (ontology.getSource().equals(currentOntology.getSource())
                    && ontology.getSourceVersion().equals(currentOntology.getSourceVersion())
                    && ontology.getId().equals(currentOntology.getId())
                    && ontology.getIdVersion().equals(currentOntology.getIdVersion())) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getOntologies().add(ontology);
        }
    }

    public List<Publication> getPublications() {
        return publications;
    }

    public void setPublications(List<Publication> publications) {
        this.publications = publications;
    }

    public void setPublication(Publication publication) {
        // Adding publication unless it exists
        boolean duplicate = false;
        for (Publication currentPublication : this.getPublications()) {
            if (publication.getSource().equals(currentPublication.getSource())
                    && publication.getId().equals(currentPublication.getId())) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getPublications().add(publication);
        }
    }
}
