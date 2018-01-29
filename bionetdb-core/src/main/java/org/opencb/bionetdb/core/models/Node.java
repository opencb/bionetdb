package org.opencb.bionetdb.core.models;

import org.apache.avro.generic.GenericData;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

public class Node {

    protected int uid;
    protected String id;
    protected String name;

    protected Type type;
    protected List<String> labels;

    protected ObjectMap attributes;

    public enum Type {
        UNDEFINED           ("UNDEFINED"),
        PHYSICAL_ENTITY     ("PHYSICAL_ENTITY"),
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
        PROTEIN_ANNOTATION("PROTEIN_ANNOTATION"),
        SUBST_SCORE("SUBST_SCORE"),
        PROTEIN_FEATURE("PROTEIN_FEATURE"),
        TRAIT_ASSOCIATION("TRAIT_ASSOCIATION"),
        VARIANT_ANNOTATION("VARIANT_ANNOTATION"),
        GENE_ANNOTATION("GENE_ANNOTATION"),
        GENE_TRAIT_ASSOCIATION("GENE_TRAIT_ASSOCIATION"),
        DISEASE("DISEASE"),
        DRUG("DRUG"),
        EXPRESION("EXPRESSION");

        private final String type;

        Type(String type) {
            this.type = type;
        }

    }

    public static boolean isPhysicalEntity(Node node) {
        switch (node.type) {
            case UNDEFINED:
            case PHYSICAL_ENTITY:
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
        labels = new ArrayList<>();
        attributes = new ObjectMap();
    }

    public Node(int uid, String id, String name, Type type) {
        this.type = type;
        labels = new ArrayList<>();

        attributes = new ObjectMap();

        setUid(uid);
        setId(id);
        setName(name);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Node{");
        sb.append("uid='").append(uid).append('\'');
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", labels=").append(labels);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public int getUid() {
        return uid;
    }

    public Node setUid(int uid) {
        this.uid = uid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Node setId(String id) {
        this.id = id;
        addAttribute("id", id);
        return this;
    }

    public String getName() {
        return name;
    }

    public Node setName(String name) {
        this.name = name;
        addAttribute("name", name);
        return this;
    }

    public Type getType() {
        return type;
    }

    public Node setType(Type type) {
        this.type = type;
        return this;
    }

    public List<String> getLabels() {
        return labels;
    }

    public Node setLabels(List<String> labels) {
        this.labels = labels;
        return this;
    }

    public void addLabel(String label) {
        labels.add(label);
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
}
