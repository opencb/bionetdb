package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by imedina on 10/08/15.
 */
public class PhysicalEntity {

    protected String id;
    protected String name;
    protected List<String> description;
    protected List<CellularLocation> cellularLocation;
    protected List<String> source;
    protected List<String> members;
    protected List<String> memberOfSet;
    protected List<String> componentOfComplex;
    protected List<String> participantOfInteraction;
    protected List<Map<String, Object>> features;
    protected List<Xref> xrefs;
    protected List<Ontology> ontologies;
    protected List<Publication> publications;

    protected Type type;

    protected Map<String, Object> attributes;

    // TODO think about this!
    protected Display display;

    public enum Type {
        UNDEFINED      ("undefined"),
        PROTEIN        ("protein"),
        DNA            ("dna"),
        RNA            ("rna"),
        COMPLEX        ("complex"),
        SMALL_MOLECULE ("smallMolecule");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public PhysicalEntity() {
        this.id = "";
        this.name = "";
        this.description = new ArrayList<>();

        // init rest of attributes
        init();
    }

    public PhysicalEntity(String id, String name, List<String> description, Type type) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;

        // init rest of attributes
        init();
    }

    private void init() {
        this.attributes = new HashMap<>();
        this.cellularLocation = new ArrayList<>();
        this.source = new ArrayList<>();
        this.members = new ArrayList<>();
        this.memberOfSet = new ArrayList<>();
        this.componentOfComplex = new ArrayList<>();
        this.participantOfInteraction = new ArrayList<>();
        this.features = new ArrayList<>();
        this.xrefs = new ArrayList<>();
        this.publications = new ArrayList<>();
        this.ontologies = new ArrayList<>();
    }

    class Display {

        private int x;
        private int y;
        private int z;

    }

    public boolean isEqual(PhysicalEntity that) {
        // sharing common xref
        boolean xrefShared = false;
        mainLoop:
        for (Xref xrefThis : this.getXrefs()) {
            for (Xref xrefThat : that.getXrefs()) {
                if (xrefThis.getId().equals(xrefThat.getId())
                        && xrefThis.getSource().equals(xrefThat.getSource())
                        && xrefThis.getIdVersion().equals(xrefThat.getIdVersion())
                        && xrefThis.getSourceVersion().equals(xrefThat.getSourceVersion())) {
                    xrefShared = true;
                    break mainLoop;
                }
            }
        }
        if (!xrefShared) {
            return false;
        }

        // sharing common location
        boolean clShared = false;
        mainLoop:
        for (CellularLocation clThis : this.getCellularLocation()) {
            for (CellularLocation clThat : that.getCellularLocation()) {
                if (clThis.getName().equals(clThat.getName())) {
                    clShared = true;
                    break mainLoop;
                }
            }
        }
        if (!clShared) {
            return false;
        }
        return true;
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

    public List<CellularLocation> getCellularLocation() {
        return cellularLocation;
    }

    public void setCellularLocation(List<CellularLocation> cellularLocation) {
        this.cellularLocation = cellularLocation;
    }

    public List<String> getSource() {
        return source;
    }

    public void setSource(List<String> source) {
        this.source = source;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public List<String> getMembers() {

        return members;
    }

    public void setMembers(List<String> members) {
        this.members = members;
    }

    public List<String> getMemberOfSet() {
        return memberOfSet;
    }

    public void setMemberOfSet(List<String> memberOfSet) {
        this.memberOfSet = memberOfSet;
    }

    public List<String> getComponentOfComplex() {
        return componentOfComplex;
    }

    public void setComponentOfComplex(List<String> componentOfComplex) {
        this.componentOfComplex = componentOfComplex;
    }

    public List<String> getParticipantOfInteraction() {
        return participantOfInteraction;
    }

    public void setParticipantOfInteraction(List<String> participantOfInteraction) {
        this.participantOfInteraction = participantOfInteraction;
    }

    public List<Map<String, Object>> getFeatures() {
        return features;
    }

    public void setFeatures(List<Map<String, Object>> features) {
        this.features = features;
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
        for (Xref thisXref : this.getXrefs()) {
            if (thisXref.isEqual(xref)) {
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
        for (Ontology thisOntology : this.getOntologies()) {
            if (thisOntology.isEqual(ontology)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getOntologies().add(ontology);
        }
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
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
        for (Publication thisPublication : this.getPublications()) {
            if (thisPublication.isEqual(publication)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getPublications().add(publication);
        }
    }
}
