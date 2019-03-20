package org.opencb.bionetdb.core.neo4j.query;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

public final class Neo4JVariantQueryParam  implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<Neo4JVariantQueryParam> VALUES = new ArrayList<>();
    private static final Map<String, Neo4JVariantQueryParam> VALUES_MAP = new HashMap<>();

    public static final String PANEL_DESCR
            = "List of gene panels";
    public static final Neo4JVariantQueryParam PANEL = new Neo4JVariantQueryParam("panel", TEXT_ARRAY, PANEL_DESCR);

    public static final String GENOTYPE_DESCR
            = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)*"
            + " e.g. HG0097:0/0;HG0098:0/1,1/1. "
            + "Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS "
            + " e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. "
            + "This will automatically set 'includeSample' parameter when not provided";
    public static final Neo4JVariantQueryParam GENOTYPE = new Neo4JVariantQueryParam("genotype", TEXT_ARRAY, GENOTYPE_DESCR);

    public static final String ANNOT_BIOTYPE_DESCR
            = "List of biotypes, e.g. protein_coding";
    public static final Neo4JVariantQueryParam ANNOT_BIOTYPE = new Neo4JVariantQueryParam("biotype", TEXT_ARRAY, ANNOT_BIOTYPE_DESCR);

    public static final String ANNOT_CONSEQUENCE_TYPE_DESCR
            = "List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578";
    public static final Neo4JVariantQueryParam ANNOT_CONSEQUENCE_TYPE = new Neo4JVariantQueryParam("ct", TEXT_ARRAY,
            ANNOT_CONSEQUENCE_TYPE_DESCR);

    public static final String ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR
            = "Alternate Population Frequency: {population}[<|>|<=|>=]{number}. e.g. ALL<0.01";
    public static final Neo4JVariantQueryParam ANNOT_POPULATION_ALTERNATE_FREQUENCY
            = new Neo4JVariantQueryParam("populationFrequencyAlt", TEXT_ARRAY, ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR);

    private Neo4JVariantQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;

        VALUES.add(this);
        VALUES_MAP.put(key, this);
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return key() + " [" + type() + "] : " + description();
    }

    public static List<Neo4JVariantQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }

    public static Neo4JVariantQueryParam valueOf(String param) {
        return VALUES_MAP.get(param);
    }
}
