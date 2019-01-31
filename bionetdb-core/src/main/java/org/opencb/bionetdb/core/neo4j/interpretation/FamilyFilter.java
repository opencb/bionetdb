package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;

public class FamilyFilter {

    private Pedigree pedigree;
    private Phenotype phenotype;
    private ClinicalProperty.ModeOfInheritance moi;
    private ClinicalProperty.Penetrance penetrance;

    public FamilyFilter(Pedigree pedigree, Phenotype phenotype) {
        this.pedigree = pedigree;
        this.phenotype = phenotype;
    }

    public FamilyFilter(Pedigree pedigree, Phenotype phenotype, ClinicalProperty.ModeOfInheritance moi,
                        ClinicalProperty.Penetrance penetrance) {
        this.pedigree = pedigree;
        this.phenotype = phenotype;
        this.moi = moi;
        this.penetrance = penetrance;
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

    public ClinicalProperty.ModeOfInheritance getMoi() {
        return moi;
    }

    public FamilyFilter setMoi(ClinicalProperty.ModeOfInheritance moi) {
        this.moi = moi;
        return this;

    }

    public ClinicalProperty.Penetrance getPenetrance() {
        return penetrance;
    }

    public void setPenetrance(ClinicalProperty.Penetrance penetrance) {
        this.penetrance = penetrance;
    }

    public boolean moiExists() {
        return (this.getMoi() != null);
    }

    public boolean penetranceExists() {
        return (this.getPenetrance() != null);
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
