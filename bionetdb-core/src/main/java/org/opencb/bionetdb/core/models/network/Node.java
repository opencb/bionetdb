package org.opencb.bionetdb.core.models.network;

import org.apache.commons.collections4.MapUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Node {

    private long uid;

    private String id;
    private String name;

    private List<Label> labels;

    private ObjectMap attributes;

    private static long counter = 0;

    public enum Label {
        INTERNAL_CONNFIG,

        UNDEFINED,

        PHYSICAL_ENTITY,
        TRANSCRIPT,
        ENSEMBL_TRANSCRIPT,
        REFSEQ_TRANSCRIPT,
        EXON,
        ENSEMBL_EXON,
        REFSEQ_EXON,
        PROTEIN,
        PHYSICAL_ENTITY_COMPLEX,
        RNA,
        SMALL_MOLECULE,

        MIRNA,
        MIRNA_MATURE,
        MIRNA_TARGET,

        DNA,
        GENE,
        ENSEMBL_GENE,
        REFSEQ_GENE,
        VARIANT,
        REGULATION_REGION,
        TFBS,

        HGV,

        PANEL_GENE,
        PANEL_VARIANT,
        PANEL_STR,
        PANEL_REGION,

        PROTEIN_ANNOTATION,
        PROTEIN_FEATURE,

        STRUCTURAL_VARIATION,
        BREAKEND,
        BREAKEND_MATE,
        VARIANT_ANNOTATION,
        EVIDENCE_SUBMISSION,
        HERITABLE_TRAIT,
        PROPERTY,
        REPEAT,
        CYTOBAND,
        VARIANT_CONSEQUENCE_TYPE,
        SO_TERM,
        VARIANT_DRUG_INTERACTION,
        VARIANT_POPULATION_FREQUENCY,
        VARIANT_CONSERVATION_SCORE,
        VARIANT_FUNCTIONAL_SCORE,
        PROTEIN_VARIANT_ANNOTATION,
        PROTEIN_SUBSTITUTION_SCORE,
        VARIANT_CLASSIFICATION,

        GENE_ANNOTATION,
        CLINICAL_EVIDENCE,
        GENE_TRAIT_ASSOCIATION,
        GENE_DRUG_INTERACTION,
        DRUG,
        GENE_EXPRESSION,
        ONTOLOGY,
        FEATURE_ONTOLOGY_TERM_ANNOTATION,
        TRANSCRIPT_ANNOTATION_EVIDENCE,

        TRANSCRIPT_CONSTRAINT_SCORE,

        PATHWAY,

        CELLULAR_LOCATION,
        REGULATION,
        CATALYSIS,
        REACTION,
        COMPLEX_ASSEMBLY,
        TRANSPORT,
        INTERACTION,

        VARIANT_FILE,
        VARIANT_FILE_DATA,
        SAMPLE,
        VARIANT_SAMPLE_DATA,
        INDIVIDUAL,
        FAMILY,

        TRANSCRIPT_ANNOTATION_FLAG,
        EXON_OVERLAP,
        PROTEIN_KEYWORD,

        DISEASE_PANEL,
        GENOMIC_FEATURE,

        DISORDER,
        PHENOTYPE,
        ONTOLOGY_TERM,

        ASSEMBLY,

        CUSTOM,

        XREF
    }

    public Node() {
        this(-1, null, null, Collections.emptyList());
    }

    public Node(long uid) {
        this(uid, null, null, Collections.emptyList());
    }

    public Node(long uid, String id, String name, Label label) {
        this.labels = new ArrayList<>();
        this.labels.add(label);

        attributes = new ObjectMap();

        setUid(uid);
        setId(id);
        setName(name);

        counter++;
    }

    public Node(long uid, String id, String name, List<Label> labels) {
        this.labels = labels;

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
        sb.append(", labels=").append(labels);
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
        sb.append(", labels=").append(labels);
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

    public List<Label> getLabels() {
        return labels;
    }

    public Node setLabels(List<Label> labels) {
        this.labels = labels;
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

    public static long getCounter() {
        return counter;
    }
}
