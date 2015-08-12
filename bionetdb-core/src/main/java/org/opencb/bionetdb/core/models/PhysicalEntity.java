package org.opencb.bionetdb.core.models;

import java.util.Collection;
import java.util.Map;

/**
 * Created by imedina on 10/08/15.
 */
public class PhysicalEntity {

    protected String id;
    protected String name;
    protected String description;
    protected Collection cellularLocation;
    protected Collection dataSource;
    protected Collection altNames;
    protected Collection participantOf;

    protected Type type;

    protected Map<String, Collection> attributes;

    // TODO think about his!
    protected Display display;

    enum Type {
        PROTEIN       ("protein"),
        DNA           ("dna"),
        RNA           ("rna"),
        COMPLEX       ("complex"),
        SMALLMOLECULE ("smallmolecule");

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

    public Collection getCellularLocation() {
        return cellularLocation;
    }

    public void setCellularLocation(Collection cellularLocation) {
        this.cellularLocation = cellularLocation;
    }

    public Collection getDataSource() {
        return dataSource;
    }

    public void setDataSource(Collection dataSource) {
        this.dataSource = dataSource;
    }

    public Collection getAltNames() {
        return altNames;
    }

    public void setAltNames(Collection altNames) {
        this.altNames = altNames;
    }

    public Collection getParticipantOf() {
        return participantOf;
    }

    public void setParticipantOf(Collection participantOf) {
        this.participantOf = participantOf;
    }

    public Map<String, Collection> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Collection> attributes) {
        this.attributes = attributes;
    }
}
