package org.opencb.bionetdb.core;

import org.opencb.biodata.models.variant.Variant;

public class VariantsPair {
    private Variant variant1;
    private Variant variant2;
    private String gene;

    public VariantsPair() {
    }

    public VariantsPair(Variant variant1, Variant variant2, String gene) {
        this.variant1 = variant1;
        this.variant2 = variant2;
        this.gene = gene;
    }

    public Variant getVariant1() {
        return variant1;
    }

    public VariantsPair setVariant1(Variant variant1) {
        this.variant1 = variant1;
        return this;
    }

    public Variant getVariant2() {
        return variant2;
    }

    public VariantsPair setVariant2(Variant variant2) {
        this.variant2 = variant2;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public VariantsPair setGene(String gene) {
        this.gene = gene;
        return this;
    }
}
