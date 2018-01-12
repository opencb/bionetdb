package org.opencb.bionetdb.core.models;

import org.opencb.commons.datastore.core.ObjectMap;

public class Node {
    protected String id;
    protected String name;
    protected ObjectMap attributes;

    protected Type type;

    //protected List<Type> subtypes;

    public enum Type {
        UNDEFINED           ("UNDEFINED"),
        PROTEIN             ("PROTEIN"),
        GENE                ("GENE"),
        TRANSCRIPT          ("TRANSCRIPT"),
        VARIANT             ("VARIANT"),
        DNA                 ("DNA"),
        RNA                 ("RNA"),
        COMPLEX             ("COMPLEX"),
        SMALL_MOLECULE      ("SMALL_MOLECULE"),
        CONSEQUENCE_TYPE    ("CONSEQUENCE_TYPE"),
        SO                  ("SEQUENCE_ONTOLOGY_TERM"),
        XREF                ("XREF"),
        POPULATION_FREQUENCY("POPULATION_FREQUENCY"),
        CONSERVATION        ("CONSERVATION"),
        FUNCTIONAL_SCORE    ("FUNCTIONAL_SCORE"),
        PROTEIN_VARIANT_ANNOTATION("PROTEIN_VARIANT_ANNOTATION"),
        SUBST_SCORE("SUBST_SCORE"),
        PROTEIN_FEATURE("PROTEIN_FEATURE"),
        TRAIT_ASSOCIATION("TRAIT_ASSOCIATION");

        private final String type;

        Type(String type) {
            this.type = type;
        }

    }

    public static boolean isPhysicalEntity(Node node) {
        switch (node.type) {
            case UNDEFINED:
            case PROTEIN:
            case GENE:
            case DNA:
            case RNA:
            case COMPLEX:
            case SMALL_MOLECULE:
                return true;
            default:
                return false;
        }
    }

    public Node() {
        attributes = new ObjectMap();
    }

    public Node(String id, String name, Type type) {
        this.type = type;
        attributes = new ObjectMap();

        setId(id);
        setName(name);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        addAttribute("id", id);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        addAttribute("name", name);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

//    public List<Type> getSubtypes() {
//        return subtypes;
//    }
//
//    public Node setSubtypes(List<Type> subtypes) {
//        this.subtypes = subtypes;
//        return this;
//    }

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
}
