package org.opencb.bionetdb.core.models.network;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.*;

public class Node {

    private long uid;

    private String id;
    private String name;

    private Type type;
    private List<String> tags;

    private ObjectMap attributes;

    private static long counter = 0;

    public enum Type {
        CONFIG("CONFIG"),

        UNDEFINED("UNDEFINED"),

        PHYSICAL_ENTITY("PHYSICAL_ENTITY"),
        TRANSCRIPT("TRANSCRIPT", "PHYSICAL_ENTITY"),
        ENSEMBL_TRANSCRIPT("ENSEMBL_TRANSCRIPT", "TRANSCRIPT,PHYSICAL_ENTITY"),
        REFSEQ_TRANSCRIPT("REFSEQ_TRANSCRIPT", "TRANSCRIPT,PHYSICAL_ENTITY"),
        EXON("EXON", "PHYSICAL_ENTITY"),
        ENSEMBL_EXON("ENSEMBL_EXON", "EXON,PHYSICAL_ENTITY"),
        REFSEQ_EXON("REFSEQ_EXON", "EXON,PHYSICAL_ENTITY"),
        PROTEIN("PROTEIN", "PHYSICAL_ENTITY"),
        COMPLEX("COMPLEX", "PHYSICAL_ENTITY"),
        RNA("RNA", "PHYSICAL_ENTITY"),
        SMALL_MOLECULE ("SMALL_MOLECULE", "PHYSICAL_ENTITY"),

        MIRNA("MIRNA", "PHYSICAL_ENTITY"),
        MIRNA_MATURE("MIRNA_MATURE", "PHYSICAL_ENTITY"),
//        TARGET_TRANSCRIPT   ("TARGET_TRANSCRIPT"),

        DNA("DNA", "PHYSICAL_ENTITY"),    // ~= GENOMIC_FEATURE
        GENE("GENE", "PHYSICAL_ENTITY"),
        ENSEMBL_GENE("ENSEMBL_GENE", "GENE,PHYSICAL_ENTITY"),
        REFSEQ_GENE("REFSEQ_GENE", "GENE,PHYSICAL_ENTITY"),
        VARIANT("VARIANT", "PHYSICAL_ENTITY"),
        REGULATION_REGION("REGULATION_REGION", "DNA,PHYSICAL_ENTITY"),
        TFBS("TFBS", "REGULATION_REGION,DNA,PHYSICAL_ENTITY"),

        XREF("XREF"),

        PANEL_GENE("PANEL_GENE"),
        PANEL_VARIANT("PANEL_VARIANT"),
        PANEL_STR("PANEL_STR"),
        PANEL_REGION("PANEL_REGION"),

        PROTEIN_ANNOTATION("PROTEIN_ANNOTATION"),
        PROTEIN_FEATURE("PROTEIN_FEATURE"),

        VARIANT_ANNOTATION("VARIANT_ANNOTATION"),
        CONSEQUENCE_TYPE("CONSEQUENCE_TYPE"),
        SO("SO"),
        POPULATION_FREQUENCY("POPULATION_FREQUENCY"),
        CONSERVATION("CONSERVATION"),
        FUNCTIONAL_SCORE("FUNCTIONAL_SCORE"),
        PROTEIN_VARIANT_ANNOTATION("PROTEIN_VARIANT_ANNOTATION"),
        SUBSTITUTION_SCORE("SUBSTITUTION_SCORE"),

        GENE_ANNOTATION("GENE_ANNOTATION"),
        TRAIT_ASSOCIATION("TRAIT_ASSOCIATION"),
        GENE_TRAIT_ASSOCIATION("GENE_TRAIT_ASSOCIATION"),
        DISEASE("DISEASE"),
        DRUG("DRUG", "PHYSICAL_ENTITY"),
        EXPRESSION("EXPRESSION"),
        ONTOLOGY("ONTOLOGY"),
        FEATURE_ONTOLOGY_TERM_ANNOTATION("FEATURE_ONTOLOGY_TERM_ANNOTATION"),
        ANNOTATION_EVIDENCE("ANNOTATION_EVIDENCE"),

        CONSTRAINT("CONSTRAINT"),

        PATHWAY ("PATHWAY"),

        CELLULAR_LOCATION ("CELLULAR_LOCATION"),
        REGULATION("REGULATION", "INTERACTION"),
        CATALYSIS("CATALYSIS", "INTERACTION"),
        REACTION("REACTION", "INTERACTION"),
        ASSEMBLY("ASSEMBLY"),
        TRANSPORT("TRANSPORT"),
        INTERACTION("INTERACTION"),

        FILE("FILE"),
        SAMPLE("SAMPLE"),
        INDIVIDUAL("INDIVIDUAL"),
        FAMILY("FAMILY"),
        VARIANT_CALL("VARIANT_CALL"),
        VARIANT_FILE_INFO("VARIANT_FILE_INFO"),
        EXPERIMENT("EXPERIMENT"),

        TRANSCRIPT_ANNOTATION_FLAG("TRANSCRIPT_ANNOTATION_FLAG"),
        EXON_OVERLAP("EXON_OVERLAP"),
        PROTEIN_KEYWORD("PROTEIN_KEYWORD"),

        PANEL("PANEL"),
        GENOMIC_FEATURE("GENOMIC_FEATURE", "DNA,PHYSICAL_ENTITY"),

        DISORDER("DISORDER"),
        PHENOTYPE("PHENOTYPE"),
        ONTOLOGY_TERM("ONTOLOGY_TERM"),

        VARIANT_OBJECT("VARIANT_OBJECT"),
        GENE_OBJECT("GENE_OBJECT"),
        PROTEIN_OBJECT("PROTEIN_OBJECT"),

        CLINICAL_ANALYSIS("CLINICAL_ANALYSIS"),
        INTERPRETATION("INTERPRETATION"),
        REPORTED_VARIANT("REPORTED_VARIANT"),
        REPORTED_EVENT("REPORTED_EVENT"),
        VARIANT_CLASSIFICATION("VARIANT_CLASSIFICATION"),
        ANALYST("ANALYST"),
        CLINICAL_ANALYST("CLINICAL_ANALYST"),
        SOFTWARE("SOFTWARE"),
        LOW_COVERAGE_REGION("LOW_COVERAGE_REGION"),
        COMMENT("COMMENT"),

        ALERT("ALERT");

        private String type;
        private List<String> labels;

        Type(String type) {
            this(type, null);
        }

        Type(String type, String labels) {
            Set set = new HashSet<String>();
            set.add(type);
            if (StringUtils.isNotEmpty(labels)) {
                set.addAll(Arrays.asList(labels.split(",")));
            }
            this.type = type;
            this.labels = new ArrayList<>(set);
        }

        public List<String> getLabels() {
            return labels;
        }

        public List<Type> getAll() {
            List<Type> list = new ArrayList<>();
            return list;
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
        this(-1, null, null, null);
    }

    public Node(long uid) {
        this(uid, null, null, null);
    }

    public Node(long uid, String id, String name, Type type) {
        this.type = type;

        tags = new ArrayList<>(1);
        if (type != null && StringUtils.isNotEmpty(type.name())) {
            tags.add(type.name());
        }

        attributes = new ObjectMap();

        setUid(uid);
        setId(id);
        setName(name);

        counter++;
    }

    public String toStringEx() {
        final StringBuilder sb = new StringBuilder("Node{");
        sb.append("uid='").append(uid).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", tags=").append(tags);
        sb.append(", attributes={");
        if (MapUtils.isNotEmpty(attributes)) {
            for (String key: attributes.keySet()) {
                sb.append(key).append("=").append(attributes.get(key)).append(',');
            }
        }
        sb.append("}}");
        return sb.toString();
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

    public static long getCounter() {
        return counter;
    }
}
