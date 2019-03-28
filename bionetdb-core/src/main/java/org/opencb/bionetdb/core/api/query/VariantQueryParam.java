package org.opencb.bionetdb.core.api.query;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.BOOLEAN;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

public final class VariantQueryParam implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final List<VariantQueryParam> VALUES = new ArrayList<>();
    private static final Map<String, VariantQueryParam> VALUES_MAP = new HashMap<>();

    public static final String CHROMOSOME_DESCR = "List of chromosomes";
    public static final VariantQueryParam CHROMOSOME = new VariantQueryParam("chromosome", TEXT_ARRAY, CHROMOSOME_DESCR);

    public static final String PANEL_DESCR = "List of gene panels";
    public static final VariantQueryParam PANEL = new VariantQueryParam("panel", TEXT_ARRAY, PANEL_DESCR);

    public static final String GENE_DESCR = "List of genes";
    public static final VariantQueryParam GENE = new VariantQueryParam("gene", TEXT_ARRAY, GENE_DESCR);

    public static final String GENOTYPE_DESCR
            = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)*"
            + " e.g. HG0097:0/0;HG0098:0/1,1/1. "
            + "Genotype aliases accepted: HOM_REF, HOM_ALT, HET, HET_REF, HET_ALT and MISS "
            + " e.g. HG0097:HOM_REF;HG0098:HET_REF,HOM_ALT. "
            + "This will automatically set 'includeSample' parameter when not provided";
    public static final VariantQueryParam GENOTYPE = new VariantQueryParam("genotype", TEXT_ARRAY, GENOTYPE_DESCR);

    public static final String ANNOT_BIOTYPE_DESCR = "List of biotypes, e.g. protein_coding";
    public static final VariantQueryParam ANNOT_BIOTYPE = new VariantQueryParam("biotype", TEXT_ARRAY, ANNOT_BIOTYPE_DESCR);

    public static final String ANNOT_CONSEQUENCE_TYPE_DESCR
            = "List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578";
    public static final VariantQueryParam ANNOT_CONSEQUENCE_TYPE = new VariantQueryParam("ct", TEXT_ARRAY,
            ANNOT_CONSEQUENCE_TYPE_DESCR);

    public static final String ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR
            = "Alternate Population Frequency: {population}[<|>|<=|>=]{number}. e.g. ALL<0.01";
    public static final VariantQueryParam ANNOT_POPULATION_ALTERNATE_FREQUENCY
            = new VariantQueryParam("populationFrequencyAlt", TEXT_ARRAY, ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR);

    // INCLUDE

    public static final String INCLUDE_STUDY_DESCR = "Include study";
    public static final VariantQueryParam INCLUDE_STUDY
            = new VariantQueryParam("includeStudy", BOOLEAN, INCLUDE_STUDY_DESCR);

    public static final String INCLUDE_CONSEQUENCE_TYPE_DESCR = "Include consequence types";
    public static final VariantQueryParam INCLUDE_CONSEQUENCE_TYPE
            = new VariantQueryParam("includeConsequenceType", BOOLEAN, INCLUDE_CONSEQUENCE_TYPE_DESCR);

    public static final String INCLUDE_POPULATION_FREQUENCY_DESCR = "Include population frequencies";
    public static final VariantQueryParam INCLUDE_POPULATION_FREQUENCY
            = new VariantQueryParam("includePopulationFrequency", BOOLEAN, INCLUDE_POPULATION_FREQUENCY_DESCR);

    public static final String INCLUDE_CONSERVATION_DESCR = "Include conservation";
    public static final VariantQueryParam INCLUDE_CONSERVATION
            = new VariantQueryParam("includeConservation", BOOLEAN, INCLUDE_CONSERVATION_DESCR);

    public static final String INCLUDE_GENE_EXPRESSION_DESCR = "Include gene expressions";
    public static final VariantQueryParam INCLUDE_GENE_EXPRESSION
            = new VariantQueryParam("includeGeneExpression", BOOLEAN, INCLUDE_GENE_EXPRESSION_DESCR);

    public static final String INCLUDE_GENE_TRAIT_ASSOCIATION_DESCR = "Include gene trait associations";
    public static final VariantQueryParam INCLUDE_GENE_TRAIT_ASSOCIATION
            = new VariantQueryParam("includeGeneTraitAssociation", BOOLEAN, INCLUDE_GENE_TRAIT_ASSOCIATION_DESCR);

    public static final String INCLUDE_GENE_DRUG_INTERACTION_DESCR = "Include gene drug interactions";
    public static final VariantQueryParam INCLUDE_GENE_DRUG_INTERACTION
            = new VariantQueryParam("includeGeneDrugInteraction", BOOLEAN, INCLUDE_GENE_DRUG_INTERACTION_DESCR);

    public static final String INCLUDE_VARIANT_TRAIT_ASSOCIATION_DESCR = "Include variant trait associations";
    public static final VariantQueryParam INCLUDE_VARIANT_TRAIT_ASSOCIATION
            = new VariantQueryParam("includeVariantTraitAssociation", BOOLEAN, INCLUDE_VARIANT_TRAIT_ASSOCIATION_DESCR);

    public static final String INCLUDE_TRAIT_ASSOCIATION_DESCR = "Include trait associations";
    public static final VariantQueryParam INCLUDE_TRAIT_ASSOCIATION
            = new VariantQueryParam("includeTraitAssociation", BOOLEAN, INCLUDE_TRAIT_ASSOCIATION_DESCR);

    public static final String INCLUDE_FUNCTIONAL_SCORE_DESCR = "Include function scores";
    public static final VariantQueryParam INCLUDE_FUNCTIONAL_SCORE
            = new VariantQueryParam("includeFunctionalScore", BOOLEAN, INCLUDE_FUNCTIONAL_SCORE_DESCR);

    // EXCLUDE

    public static final String EXCLUDE_STUDY_DESCR = "Exclude studies";
    public static final VariantQueryParam EXCLUDE_STUDY
            = new VariantQueryParam("excludeStudy", BOOLEAN, EXCLUDE_STUDY_DESCR);

    public static final String EXCLUDE_CONSEQUENCE_TYPE_DESCR = "Exclude consequence types";
    public static final VariantQueryParam EXCLUDE_CONSEQUENCE_TYPE
            = new VariantQueryParam("excludeConsequenceType", BOOLEAN, EXCLUDE_CONSEQUENCE_TYPE_DESCR);

    public static final String EXCLUDE_POPULATION_FREQUENCY_DESCR = "Exclude population frequencies";
    public static final VariantQueryParam EXCLUDE_POPULATION_FREQUENCY
            = new VariantQueryParam("excludePopulationFrequency", BOOLEAN, EXCLUDE_POPULATION_FREQUENCY_DESCR);

    public static final String EXCLUDE_CONSERVATION_DESCR = "Exclude conservations";
    public static final VariantQueryParam EXCLUDE_CONSERVATION
            = new VariantQueryParam("excludeConservation", BOOLEAN, EXCLUDE_CONSERVATION_DESCR);

    public static final String EXCLUDE_GENE_EXPRESSION_DESCR = "Exclude gene expressions";
    public static final VariantQueryParam EXCLUDE_GENE_EXPRESSION
            = new VariantQueryParam("excludeGeneExpression", BOOLEAN, EXCLUDE_GENE_EXPRESSION_DESCR);

    public static final String EXCLUDE_GENE_TRAIT_ASSOCIATION_DESCR = "Exclude gene trait associations";
    public static final VariantQueryParam EXCLUDE_GENE_TRAIT_ASSOCIATION
            = new VariantQueryParam("excludeGeneTraitAssociation", BOOLEAN, EXCLUDE_GENE_TRAIT_ASSOCIATION_DESCR);

    public static final String EXCLUDE_GENE_DRUG_INTERACTION_DESCR = "Exclude gene drug interactions";
    public static final VariantQueryParam EXCLUDE_GENE_DRUG_INTERACTION
            = new VariantQueryParam("excludeGeneDrugInteraction", BOOLEAN, EXCLUDE_GENE_DRUG_INTERACTION_DESCR);

    public static final String EXCLUDE_VARIANT_TRAIT_ASSOCIATION_DESCR = "Exclude variant trait associations";
    public static final VariantQueryParam EXCLUDE_VARIANT_TRAIT_ASSOCIATION
            = new VariantQueryParam("excludeVariantTraitAssociation", BOOLEAN, EXCLUDE_VARIANT_TRAIT_ASSOCIATION_DESCR);

    public static final String EXCLUDE_TRAIT_ASSOCIATION_DESCR = "Exclude trait associations";
    public static final VariantQueryParam EXCLUDE_TRAIT_ASSOCIATION
            = new VariantQueryParam("excludeTraitAssociation", BOOLEAN, EXCLUDE_TRAIT_ASSOCIATION_DESCR);

    public static final String EXCLUDE_FUNCTIONAL_SCORE_DESCR = "Exclude function scores";
    public static final VariantQueryParam EXCLUDE_FUNCTIONAL_SCORE
            = new VariantQueryParam("excludeFunctionalScore", BOOLEAN, EXCLUDE_FUNCTIONAL_SCORE_DESCR);

    private VariantQueryParam(String key, Type type, String description) {
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

    public static List<VariantQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }

    public static VariantQueryParam valueOf(String param) {
        return VALUES_MAP.get(param);
    }
}
