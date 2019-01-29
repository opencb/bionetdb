package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.variant.Variant;

import java.util.*;


public class VariantContainer {

    public List<Variant> complexVariantList = new ArrayList<>();
    public List<Variant> reactionVariantList = new ArrayList<>();

    public VariantContainer() {
    }

    public VariantContainer(List<Variant> complexVariantList, List<Variant> reactionVariantList) {
        this.complexVariantList = complexVariantList;
        this.reactionVariantList = reactionVariantList;
    }

    public List<Variant> getComplexVariantList() {
        return complexVariantList;
    }

    public void setComplexVariantList(List<Variant> complexVariantList) {
        this.complexVariantList = complexVariantList;
    }

    public List<Variant> getReactionVariantList() {
        return reactionVariantList;
    }

    public void setReactionVariantList(List<Variant> reactionVariantList) {
        this.reactionVariantList = reactionVariantList;
    }
}
