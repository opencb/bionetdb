package org.opencb.bionetdb.core.network;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

public class Node {

    private long uid;

    private String id;
    private String name;

    private Type type;
    private List<String> tags;

    private String source;

    private ObjectMap attributes;

    public enum Type {
        UNDEFINED           ("UNDEFINED"),

        PHYSICAL_ENTITY     ("PHYSICAL_ENTITY"),
        TRANSCRIPT          ("TRANSCRIPT", "PHYSICAL_ENTITY"),
        PROTEIN             ("PROTEIN", "PHYSICAL_ENTITY"),
        COMPLEX             ("COMPLEX", "PHYSICAL_ENTITY"),
        RNA                 ("RNA", "PHYSICAL_ENTITY"),
        SMALL_MOLECULE      ("SMALL_MOLECULE", "PHYSICAL_ENTITY"),


        DNA                 ("DNA"),    // ~= GENOMIC_FEATURE
        GENE                ("GENE"),
        VARIANT             ("VARIANT"),
        REGULATION_REGION   ("REGULATION_REGION", "DNA"),
        TFBS                ("TFBS", "REGULATION_REGION"),

        XREF                ("XREF"),

        PROTEIN_ANNOTATION  ("PROTEIN_ANNOTATION"),
        PROTEIN_FEATURE     ("PROTEIN_FEATURE"),

        VARIANT_ANNOTATION          ("VARIANT_ANNOTATION"),
        CONSEQUENCE_TYPE            ("CONSEQUENCE_TYPE"),
        SO                          ("SEQUENCE_ONTOLOGY_TERM"),
        POPULATION_FREQUENCY        ("POPULATION_FREQUENCY"),
        CONSERVATION                ("CONSERVATION"),
        FUNCTIONAL_SCORE            ("FUNCTIONAL_SCORE"),
        PROTEIN_VARIANT_ANNOTATION  ("PROTEIN_VARIANT_ANNOTATION"),
        SUBSTITUTION_SCORE          ("SUBSTITUTION_SCORE"),

        GENE_ANNOTATION         ("GENE_ANNOTATION"),
        TRAIT_ASSOCIATION       ("TRAIT_ASSOCIATION"),
        GENE_TRAIT_ASSOCIATION  ("GENE_TRAIT_ASSOCIATION"),
        DISEASE                 ("DISEASE"),
        DRUG                    ("DRUG"),
        EXPRESSION              ("EXPRESSION"),
        ONTOLOGY                ("ONTOLOGY"),


        CELLULAR_LOCATION   ("CELLULAR_LOCATION"),
        REGULATION          ("REGULATION"),
        CATALYSIS           ("CATALYSIS"),
        REACTION            ("REACTION"),
        ASSEMBLY            ("ASSEMBLY"),
        TRANSPORT           ("TRANSPORT"),
        INTERACTION         ("INTERACTION"),

        SAMPLE("SAMPLE"),
        VARIANT_CALL("VARIANT_CALL"),
        VARIANT_CALL_INFO("VARIANT_CALL_INFO");

        private final String type;
        private final String parentType;

        Type(String type) {
            this(type, null);
        }

        Type(String type, String parentType) {
            this.type = type;
            this.parentType = parentType;
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

    public Node(long uid) {
        tags = new ArrayList<>();
        attributes = new ObjectMap();

        setUid(uid);
    }

    public Node(long uid, String id, String name, Type type) {
        this(uid, id, name, type, null);
    }

    public Node(long uid, String id, String name, Type type, String source) {
        this.type = type;

        tags = new ArrayList<>(1);
        if (StringUtils.isNotEmpty(type.name())) {
            tags.add(type.name());
        }

        this.source = source;

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
        sb.append(", tags=").append(tags);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getUid() {
        return uid;
    }

    public Node setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Node setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Node setName(String name) {
        this.name = name;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Node setType(Type type) {
        this.type = type;
        addTag(type.name());
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Node setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public void addTag(String tag) {
        tags.add(tag);
    }

    public String getSource() {
        return source;
    }

    public Node setSource(String source) {
        this.source = source;
        return this;
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
