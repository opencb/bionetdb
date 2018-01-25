package org.opencb.bionetdb.core.models;

import org.opencb.commons.datastore.core.ObjectMap;

public class Relationship {
    protected String id;
    protected String name;

    protected String originId;
    protected String destId;

    protected String originType;
    protected String destType;

    protected Type type;

    protected ObjectMap attributes;

    public Relationship(String id, String originId, String originType, String destId, String destType, Type type) {
        this.id = id;
        this.originId = originId;
        this.originType = originType;
        this.destId = destId;
        this.destType = destType;
        this.type = type;

        this.attributes = new ObjectMap();
    }

    public enum Type {
        REACTION            ("reaction"),
        CATALYSIS           ("catalysis"),
        REGULATION          ("regulation"),
        COLOCALIZATION      ("colocalization"),
        CONSEQUENCE_TYPE    ("CONSEQUENCE_TYPE"),
        TRANSCRIPT          ("TRANSCRIPT"),
        SO                  ("SO"),
        GENE                ("GENE"),
        XREF                ("XREF"),
        POPULATION_FREQUENCY("POPULATION_FREQUENCY"),
        CONSERVATION        ("CONSERVATION"),
        FUNCTIONAL_SCORE    ("FUNCTIONAL_SCORE"),
        SUBST_SCORE("SUBST_SCORE"),
        PROTEIN("PROTEIN"),
        PROTEIN_FEATURE("PROTEIN_FEATURE"),
        TRAIT_ASSOCIATION("TRAIT_ASSOCIATION"),
        ANNOTATION("ANNOTATION"),
        DISEASE("DISEASE"),
        DRUG("DRUG"),
        EXPRESSION("EXPRESSION");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public static boolean isInteraction(Relationship r) {
        switch (r.type) {
            case REACTION:
            case CATALYSIS:
            case REGULATION:
            case COLOCALIZATION:
                return true;
            default:
                return false;
        }
    }

    public Relationship() {
        attributes = new ObjectMap();
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

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public void setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
    }

    public void addAttribute(String key, Object value) {
        if (key != null && value != null) {
            attributes.put(key, value);
        }
    }

    public String getOriginId() {
        return originId;
    }

    public Relationship setOriginId(String originId) {
        this.originId = originId;
        return this;
    }

    public String getDestId() {
        return destId;
    }

    public Relationship setDestId(String destId) {
        this.destId = destId;
        return this;
    }

    public String getOriginType() {
        return originType;
    }

    public Relationship setOriginType(String originType) {
        this.originType = originType;
        return this;
    }

    public String getDestType() {
        return destType;
    }

    public Relationship setDestType(String destType) {
        this.destType = destType;
        return this;
    }
}
