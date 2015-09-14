package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by imedina on 10/08/15.
 */
public class PhysicalEntity {

    protected String id;
    protected String name;
    protected String description;
    protected List<CellularLocation> cellularLocation;
    protected List<String> source;
    protected List<String> members;
    protected List<String> memberOfSet;
    protected List<String> componentOfComplex;
    protected List<String> participantOfInteraction;
    protected List<Map<String, Object>> features;
    protected List<Xref> xrefs;
    protected List<Publication> publications;

    protected Type type;

    protected Map<String, Object> attributes;

    // TODO think about his!
    protected Display display;

    public enum Type {
        UNDEFINEDENTITY ("undefinedEntity"),
        PROTEIN       ("protein"),
        DNA           ("dna"),
        RNA           ("rna"),
        COMPLEX       ("complex"),
        SMALLMOLECULE ("smallMolecule");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public PhysicalEntity() {
        // init rest of attributes
        init();
    }

    public PhysicalEntity(String id, String name, String description, Type type) {
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
    }

    class Display {

        private int x;
        private int y;
        private int z;

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description.replace("\\\"", "\'").replace("\"", "\'");
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
        for (Publication currentPublication : this.getPublications()) {
            if(publication.getSource().equals(currentPublication.getSource()) &&
                    publication.getId().equals(currentPublication.getId())) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getPublications().add(publication);
        }
    }
}
