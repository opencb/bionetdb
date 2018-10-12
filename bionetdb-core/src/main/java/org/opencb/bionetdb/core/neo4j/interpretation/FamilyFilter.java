package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.pedigree.Pedigree;

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

    public void setPedigree(Pedigree pedigree) {
        this.pedigree = pedigree;
    }

    public Phenotype getPhenotype() {
        return phenotype;
    }

    public void setPhenotype(Phenotype phenotype) {
        this.phenotype = phenotype;
    }

    public String getMoi() {
        return moi;
    }

    public void setMoi(String moi) {
        this.moi = moi;
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
