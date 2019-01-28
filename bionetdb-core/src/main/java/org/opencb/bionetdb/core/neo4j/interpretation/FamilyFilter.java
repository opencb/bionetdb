package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;

public class FamilyFilter {

    private Pedigree pedigree;
    private Phenotype phenotype;
    private String moi;

    public FamilyFilter(Pedigree pedigree, Phenotype phenotype) {
        this.pedigree = pedigree;
        this.phenotype = phenotype;
    }

    public FamilyFilter(Pedigree pedigree, Phenotype phenotype, String moi) {
        this.pedigree = pedigree;
        this.phenotype = phenotype;
        this.moi = moi;
    }

    public Pedigree getPedigree() {
        return pedigree;
    }

    public FamilyFilter setPedigree(Pedigree pedigree) {
        this.pedigree = pedigree;
        return this;
    }

    public Phenotype getPhenotype() {
        return phenotype;
    }

    public FamilyFilter setPhenotype(Phenotype phenotype) {
        this.phenotype = phenotype;
        return this;
    }

    public String getMoi() {
        return moi;
    }

    public FamilyFilter setMoi(String moi) {
        this.moi = moi;
        return this;

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyFilter{");
        sb.append("pedigree=").append(pedigree);
        sb.append(", phenotype=").append(phenotype);
        sb.append(", moi='").append(moi).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
