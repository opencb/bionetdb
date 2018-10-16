package org.opencb.bionetdb.core.neo4j.interpretation;

public class OptionsFilter {

    private boolean onlyComplex = false;
    private boolean onlyReaction = false;

    public OptionsFilter() {
    }

    public OptionsFilter(boolean onlyComplex, boolean onlyReaction) {
        this.onlyComplex = onlyComplex;
        this.onlyReaction = onlyReaction;
    }

    public boolean isOnlyComplex() {
        return onlyComplex;
    }

    public void setOnlyComplex(boolean onlyComplex) {
        this.onlyComplex = onlyComplex;
    }

    public boolean isOnlyReaction() {
        return onlyReaction;
    }

    public void setOnlyReaction(boolean onlyReaction) {
        this.onlyReaction = onlyReaction;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OptionsFilter{");
        sb.append("onlyComplex=").append(onlyComplex);
        sb.append(", onlyReaction=").append(onlyReaction);
        sb.append('}');
        return sb.toString();
    }
}