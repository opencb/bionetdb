package org.opencb.bionetdb.core.models.network;

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

    private ObjectMap attributes;

    private static long counter = 0;

    public Relation(long uid, String name, long origUid, Node.Type origType, long destUid, Node.Type destType, Type type) {
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

        this.attributes = new ObjectMap();

        counter++;
    }

    public enum Type {
        IS                  ("IS"),

        COMPONENT_OF_PATHWAY("COMPONENT_OF_PATHWAY"),
        PATHWAY_NEXT_STEP   ("PATHWAY_NEXT_STEP"),

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
        DISEASE("GENE_TRAIT_ASSOCIATION"),
        DRUG("DRUG"),
        ANNOTATION___GENE___GENE_EXPRESSION("ANNOTATION___GENE___GENE_EXPRESSION"),
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

//        RNA__MIRNA("RNA__MIRNA"),
        ANNOTATION___GENE___MIRNA_TARGET("ANNOTATION___GENE___MIRNA_TARGET"),
        ANNOTATION___MIRNA_MATURE___MIRNA_TARGET("ANNOTATION___MIRNA_MATURE___MIRNA_TARGET"),
        TARGET_GENE         ("TARGET_GENE"),
        MIRNA__TARGET_TRANSCRIPT("MIRNA__TARGET_TRANSCRIPT"),

        TARGET_TRANSCRIPT__TRANSCRIPT("TARGET_TRANSCRIPT__TRANSCRIPT"),

        IS___TRANSCRIPT___PROTEIN("IS___TRANSCRIPT___PROTEIN"),
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
        HAS___GENE___TRANSCRIPT("HAS___GENE___TRANSCRIPT"),
        ANNOTATION___GENE___GENE_DRUG_INTERACTION("ANNOTATION___GENE___GENE_DRUG_INTERACTION"),
        ANNOTATION___DRUG___GENE_DRUG_INTERACTION("ANNOTATION___DRUG___GENE_DRUG_INTERACTION"),
        ANNOTATION___GENE___GENE_TRAIT_ASSOCIATION("ANNOTATION___GENE___GENE_TRAIT_ASSOCIATION"),
        GENE__CONSTRAINT("GENE__CONSTRAINT"),
        HAS___TRANSCRIPT___EXON("HAS___TRANSCRIPT___EXON"),
        ANNOTATION___TRANSCRIPT___TFBS("ANNOTATION___TRANSCRIPT___TFBS"),
        ANNOTATION___TRANSCRIPT___TRANSCRIPT_CONSTRAINT_SCORE("ANNOTATION___TRANSCRIPT___TRANSCRIPT_CONSTRAINT_SCORE"),
        ANNOTATION___TRANSCRIPT___FEATURE_ONTOLOGY_TERM_ANNOTATION("ANNOTATION___TRANSCRIPT___FEATURE_ONTOLOGY_TERM_ANNOTATION"),
        HAS___FEATURE_ONTOLOGY_TERM_ANNOTATION___TRANSCRIPT_ANNOTATION_EVIDENCE(
                "HAS___FEATURE_ONTOLOGY_TERM_ANNOTATION___TRANSCRIPT_ANNOTATION_EVIDENCE"),
        PROTEIN_VARIANT_ANNOTATION__PROTEIN("PROTEIN_VARIANT_ANNOTATION__PROTEIN"),
        PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE("PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE"),
        PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD("PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD"),
        PROTEIN_VARIANT_ANNOTATION__PROTEIN_FEATURE("PROTEIN_VARIANT_ANNOTATION__PROTEIN_FEATURE"),
        ANNOTATION___PROTEIN___PROTEIN_KEYWORD("ANNOTATION___PROTEIN___PROTEIN_KEYWORD"),
        ANNOTATION___PROTEIN___PROTEIN_FEATURE("ANNOTATION___PROTEIN___PROTEIN_FEATURE"),
        SAMPLE__VARIANT_CALL("SAMPLE__VARIANT_CALL"),
        VARIANT_FILE_INFO__FILE("VARIANT_FILE_INFO__FILE"),
        VARIANT_CALL__VARIANT_FILE_INFO("VARIANT_CALL__VARIANT_FILE_INFO"),

        FILE__SOFTWARE("FILE__SOFTWARE"),
        FILE__EXPERIMENT("FILE__EXPERIMENT"),
        FILE__SAMPLE("FILE__SAMPLE"),

//        DNA__GENE("DNA__GENE");

        ANNOTATION___GENE___PANEL_GENE("ANNOTATION___GENE___PANEL_GENE"),
        HAS___DISEASE_PANEL___PANEL_GENE("HAS___DISEASE_PANEL___PANEL_GENE"),
        PANEL__PANEL_GENE("PANEL__PANEL_GENE"),
        PANEL__PANEL_VARIANT("PANEL__PANEL_VARIANT"),
        PANEL__PANEL_STR("PANEL__PANEL_STR"),
        PANEL__PANEL_REGION("PANEL__PANEL_REGION"),
        PANEL__DISORDER("PANEL__DISORDER"),
        PHENOTYPE__ONTOLOGY_TERM("PHENOTYPE__ONTOLOGY_TERM"),
        PANEL_GENE__GENE("PANEL_GENE__GENE"),
        PANEL_GENE__ONTOLOGY_TERM("PANEL_GENE__ONTOLOGY_TERM"),
        PANEL_VARIANT__VARIANT("PANEL_VARIANT__VARIANT"),
        PANEL_VARIANT__ONTOLOGY_TERM("PANEL_VARIANT__ONTOLOGY_TERM"),
        PANEL_STR__ONTOLOGY_TERM("PANEL_STR__ONTOLOGY_TERM"),
        PANEL_REGION__ONTOLOGY_TERM("PANEL_REGION__ONTOLOGY_TERM"),

        VARIANT__VARIANT_OBJECT("VARIANT__VARIANT_OBJECT"),
        GENE__GENE_OBJECT("GENE__GENE_OBJECT"),
        PROTEIN__PROTEIN_OBJECT("PROTEIN__PROTEIN_OBJECT"),

        FAMILY__PHENOTYPE("FAMILY__PHENOTYPE"),
        FAMILY__DISORDER("FAMILY__DISORDER"),
        FAMILY__INDIVIDUAL("FAMILY__INDIVIDUAL"),
        FATHER_OF___INDIVIDUAL___INDIVIDUAL("FATHER_OF___INDIVIDUAL___INDIVIDUAL"),
        MOTHER_OF___INDIVIDUAL___INDIVIDUAL("MOTHER_OF___INDIVIDUAL___INDIVIDUAL"),
        INDIVIDUAL__PHENOTYPE("INDIVIDUAL__PHENOTYPE"),
        INDIVIDUAL__DISORDER("INDIVIDUAL__DISORDER"),
        INDIVIDUAL__SAMPLE("INDIVIDUAL__SAMPLE"),
        DISORDER__PHENOTYPE("DISORDER__PHENOTYPE"),
        SAMPLE__PHENOTYPE("SAMPLE__PHENOTYPE"),

        CLINICAL_ANALYSIS__DISORDER("CLINICAL_ANALYSIS__DISORDER"),
        CLINICAL_ANALYSIS__FAMILY("CLINICAL_ANALYSIS__FAMILY"),
        CLINICAL_ANALYSIS__FILE("CLINICAL_ANALYSIS__FILE"),
        PROBAND___CLINICAL_ANALYSIS___INDIVIDUAL("PROBAND___CLINICAL_ANALYSIS___INDIVIDUAL"),
        CLINICAL_ANALYSIS__CLINICAL_ANALYST("CLINICAL_ANALYSIS__CLINICAL_ANALYST"),
        CLINICAL_ANALYSIS__COMMENT("CLINICAL_ANALYSIS__COMMENT"),
        CLINICAL_ANALYSIS__ALERT("CLINICAL_ANALYSIS__ALERT"),
        CLINICAL_ANALYSIS__INTERPRETATION("CLINICAL_ANALYSIS__INTERPRETATION"),

        INTERPRETATION__PANEL("INTERPRETATION__PANEL"),
        PRIMARY_FINDING___INTERPRETATION___REPORTED_VARIANT("PRIMARY_FINDING___INTERPRETATION___REPORTED_VARIANT"),
        SECONDARY_FINDING___INTERPRETATION___REPORTED_VARIANT("SECONDARY_FINDING___INTERPRETATION___REPORTED_VARIANT"),
        INTERPRETATION__COMMENT("INTERPRETATION__COMMENT"),
        INTERPRETATION__SOFTWARE("INTERPRETATION__SOFTWARE"),
        INTERPRETATION__LOW_COVERAGE_REGION("INTERPRETATION__LOW_COVERAGE_REGION"),
        REPORTED_VARIANT__VARIANT("REPORTED_VARIANT__VARIANT"),
        REPORTED_VARIANT__REPORTED_EVENT("REPORTED_VARIANT__REPORTED_EVENT"),
        REPORTED_VARIANT__COMMENT("REPORTED_VARIANT__COMMENT"),
        REPORTED_EVENT__PHENOTYPE("REPORTED_EVENT__PHENOTYPE"),
        REPORTED_EVENT__SO("REPORTED_EVENT__SO"),
        REPORTED_EVENT__GENOMIC_FEATURE("REPORTED_EVENT__GENOMIC_FEATURE"),
        REPORTED_EVENT__PANEL("REPORTED_EVENT__PANEL");

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
