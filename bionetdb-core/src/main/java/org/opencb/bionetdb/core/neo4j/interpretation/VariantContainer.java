package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.variant.Variant;

import java.util.*;


public class VariantContainer {

    private List<Variant> variantList = new ArrayList<>();
    private ClinicalProperty.ModeOfInheritance moi;
    private ClinicalProperty.Penetrance penetrance;

    public VariantContainer() {
    }

    public VariantContainer(List<Variant> variantList) {
        this.variantList = variantList;
    }

    public VariantContainer(List<Variant> variantList, ClinicalProperty.ModeOfInheritance moi,
                            ClinicalProperty.Penetrance penetrance) {
        this.variantList = variantList;
        this.moi = moi;
        this.penetrance = penetrance;
    }

    public List<Variant> getVariantList() {
        return variantList;
    }

    public void setVariantList(List<Variant> variantList) {
        this.variantList = variantList;
    }

    public ClinicalProperty.ModeOfInheritance getMoi() {
        return moi;
    }

    public void setMoi(ClinicalProperty.ModeOfInheritance moi) {
        this.moi = moi;
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
        final StringBuilder sb = new StringBuilder("VariantContainer{");
        sb.append("genericVariantList=").append(variantList);
        sb.append(", moi=").append(moi);
        sb.append(", penetrance=").append(penetrance);
        sb.append('}');
        return sb.toString();
    }
}
