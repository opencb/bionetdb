package org.opencb.bionetdb.core.models;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 10/08/15.
 */
public class PhysicalEntity {

    protected String id;
    protected String name;
    protected String description;
    protected List<String> cellularLocation;
    protected List<String> source;
    protected List<String> altNames;
    protected List<String> participantOf;

    protected Type type;

    protected Map<String, Object> attributes;

    // TODO think about his!
    protected Display display;

    enum Type {
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
    }

    public PhysicalEntity(String id, String name, String description, Type type) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
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
        this.description = description;
    }

    public List<String> getCellularLocation() {
        return cellularLocation;
    }

    public void setCellularLocation(List<String> cellularLocation) {
        this.cellularLocation = cellularLocation;
    }

    public List<String> getSource() {
        return source;
    }

    public void setSource(List<String> source) {
        this.source = source;
    }

    public List<String> getAltNames() {
        return altNames;
    }

    public void setAltNames(List<String> altNames) {
        this.altNames = altNames;
    }

    public List<String> getParticipantOf() {
        return participantOf;
    }

    public void setParticipantOf(List<String> participantOf) {
        this.participantOf = participantOf;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Display getDisplay() {
        return display;
    }

    public void setDisplay(Display display) {
        this.display = display;
    }
}
