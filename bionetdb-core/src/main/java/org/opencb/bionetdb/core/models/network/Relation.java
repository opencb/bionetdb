package org.opencb.bionetdb.core.models.network;

import org.opencb.commons.datastore.core.ObjectMap;

public class Relation {

    private long uid;

    private String name;

    private long origUid;
    private Node.Label origLabel;

    private long destUid;
    private Node.Label destLabel;

    private Label label;

    private ObjectMap attributes;

    private static long counter = 0;

    public Relation(long uid, String name, long origUid, Node.Label origLabel, long destUid, Node.Label destLabel, Label label) {
        this.uid = uid;

        this.name = name;

        this.origUid = origUid;
        this.origLabel = origLabel;

        this.destUid = destUid;
        this.destLabel = destLabel;

        this.label = label;

        this.attributes = new ObjectMap();

        counter++;
    }

    public enum Label {

        // REACTOME

        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX,
        COMPONENT_OF_PATHWAY,
        PATHWAY_NEXT_STEP,
        INTERACTION,
        REACTION,
        CATALYSIS,
        REGULATION,
        COLOCALIZATION,
        CELLULAR_LOCATION,
        CONTROLLED,
        CONTROLLER,
        COFACTOR,
        PRODUCT,
        REACTANT,

        // GENERAL

        HAS,
        MOTHER_OF,
        FATHER_OF,
        TARGET,
        IS,
        ANNOTATION,
        DATA,
        MATURE
    }

    public Relation() {
        attributes = new ObjectMap();
    }

    public Relation(long uid) {
        this.uid = uid;

        attributes = new ObjectMap();
    }

    public void addAttribute(String key, Object value) {
        if (key != null && value != null) {
            attributes.put(key, value);
        }
    }

    public long getUid() {
        return uid;
    }

    public Relation setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getName() {
        return name;
    }

    public Relation setName(String name) {
        this.name = name;
        return this;
    }

    public long getOrigUid() {
        return origUid;
    }

    public Relation setOrigUid(long origUid) {
        this.origUid = origUid;
        return this;
    }

    public Node.Label getOrigLabel() {
        return origLabel;
    }

    public long getDestUid() {
        return destUid;
    }

    public Node.Label getDestLabel() {
        return destLabel;
    }

    public Relation setDestUid(long destUid) {
        this.destUid = destUid;
        return this;
    }

    public Label getLabel() {
        return label;
    }

    public Relation setLabel(Label label) {
        this.label = label;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public Relation setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }

    public static long getCounter() {
        return counter;
    }
}
