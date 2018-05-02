package org.opencb.bionetdb.core.network;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

public class Relation {

    private long uid;

    private String name;

    private long origUid;
    private Node.Type origType;

    private long destUid;
    private Node.Type destType;

    private Type type;
    private List<String> tags;

    private String source;

    private ObjectMap attributes;

    private static long counter = 0;

    public Relation(long uid, String name, long origUid, Node.Type origType, long destUid, Node.Type destType, Type type) {
        this (uid, name, origUid, origType, destUid, destType, type, null);
    }

    public Relation(long uid, String name, long origUid, Node.Type origType, long destUid, Node.Type destType, Type type, String source) {
        this.uid = uid;

        this.name = name;

        this.origUid = origUid;
        this.origType = origType;

        this.destUid = destUid;
        this.destType = destType;

        this.type = type;
        this.tags = new ArrayList<>(1);
        if (StringUtils.isNotEmpty(type.name())) {
            tags.add(type.name());
        }

        this.source = source;

        this.attributes = new ObjectMap();

        counter++;
    }

    public enum Type {
        COMPONENT_OF_PATHWAY("COMPONENT_OF_PATHWAY"),
        PATHWAY_NEXT_STEP("PATHWAY_NEXT_STEP"),

        REACTION            ("REACTION"),
        CATALYSIS           ("CATALYSIS"),
        REGULATION          ("REGULATION"),
        COLOCALIZATION      ("COLOCALIZATION"),
        CONSEQUENCE_TYPE    ("CONSEQUENCE_TYPE"),
        TRANSCRIPT          ("TRANSCRIPT"),
        SO                  ("SO"),
        GENE                ("GENE"),
        XREF                ("XREF"),
        POPULATION_FREQUENCY("POPULATION_FREQUENCY"),
        CONSERVATION        ("CONSERVATION"),
        FUNCTIONAL_SCORE    ("FUNCTIONAL_SCORE"),
        SUBSTITUTION_SCORE("SUBSTITUTION_SCORE"),
        PROTEIN("PROTEIN"),
        PROTEIN_FEATURE("PROTEIN_FEATURE"),
        TRAIT_ASSOCIATION("TRAIT_ASSOCIATION"),
        ANNOTATION("ANNOTATION"),
        DISEASE("DISEASE"),
        DRUG("DRUG"),
        EXPRESSION("EXPRESSION"),
        INTERACTION("INTERACTION"),
        COMPONENT_OF_COMPLEX("COMPONENT_OF_COMPLEX"),
        STOICHIOMETRY("STOICHIOMETRY"),
        ONTOLOGY("ONTOLOGY"),
        CELLULAR_LOCATION("CELLULAR_LOCATION"),
        CONTROLLED("CONTROLLED"),
        CONTROLLER("CONTROLLER"),
        COFACTOR("COFACTOR"),
        PRODUCT("PRODUCT"),
        REACTANT("REACTANT"),
        VARIANT_FILE_INFO("VARIANT_FILE_INFO"),
        VARIANT_CALL("VARIANT_CALL"),

        TRANSCRIPT_ANNOTATION_FLAG("TRANSCRIPT_ANNOTATION_FLAG"),
        EXON_OVERLAP("EXON_OVERLAP"),
        PROTEIN_KEYWORD("PROTEIN_KEYWORD"),
        PROTEIN_VARIANT_ANNOTATION("PROTEIN_VARIANT_ANNOTATION"),
        TFBS("TFBS"),

        VARIANT__VARIANT_CALL("VARIANT__VARIANT_CALL"),
        VARIANT__CONSEQUENCE_TYPE("VARIANT__CONSEQUENCE_TYPE"),
        VARIANT__POPULATION_FREQUENCY("VARIANT__POPULATION_FREQUENCY"),
        VARIANT__CONSERVATION("VARIANT__CONSERVATION"),
        VARIANT__TRAIT_ASSOCIATION("VARIANT__TRAIT_ASSOCIATION"),
        VARIANT__FUNCTIONAL_SCORE("VARIANT__FUNCTIONAL_SCORE"),
        CONSEQUENCE_TYPE__SO("CONSEQUENCE_TYPE__SO"),
        CONSEQUENCE_TYPE__PROTEIN_VARIANT_ANNOTATION("CONSEQUENCE_TYPE__PROTEIN_VARIANT_ANNOTATION"),
        CONSEQUENCE_TYPE__GENE("CONSEQUENCE_TYPE__GENE"),
        CONSEQUENCE_TYPE__TRANSCRIPT("CONSEQUENCE_TYPE__TRANSCRIPT"),
        GENE__DRUG("GENE__DRUG"),
        GENE__DISEASE("GENE__DISEASE"),
        PROTEIN__PROTEIN_VARIANT_ANNOTATION("PROTEIN__PROTEIN_VARIANT_ANNOTATION"),
        PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE("PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE"),
        PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD("PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD"),
        PROTEIN_VARIANT_ANNOTATION__PROTEIN_FEATURE("PROTEIN_VARIANT_ANNOTATION__PROTEIN_FEATURE"),
        SAMPLE__VARIANT_CALL("SAMPLE__VARIANT_CALL"),
        VARIANT_CALL__VARIANT_FILE_INFO("VARIANT_CALL__VARIANT_FILE_INFO");


        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public static boolean isInteraction(Relation r) {
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

    public Relation() {
        tags = new ArrayList<>();
        attributes = new ObjectMap();
    }

    public Relation(long uid) {
        this.uid = uid;

        tags = new ArrayList<>();
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

    public String getSource() {
        return source;
    }

    public Relation setSource(String source) {
        this.source = source;
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

    public Node.Type getOrigType() {
        return origType;
    }

    public long getDestUid() {
        return destUid;
    }

    public Node.Type getDestType() {
        return destType;
    }

    public Relation setDestUid(long destUid) {
        this.destUid = destUid;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Relation setType(Type type) {
        this.type = type;
        addTag(type.name());
        return this;
    }

    public List<String> getTags() {
        return this.tags;
    }

    public Relation setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public void addTag(String tag) {
        this.tags.add(tag);
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
