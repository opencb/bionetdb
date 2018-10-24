package org.opencb.bionetdb.core.neo4j.interpretation;

import java.util.List;

public class VariantFilter {

    private List<String> listOfDiseases;
    private List<String> populationFrequencySpecies;
    private double populationFrequency;
    private List<String> consequenceType;

    public VariantFilter() {
    }

    public VariantFilter(List<String> listOfDiseases, List<String> populationFrequencySpecies, double populationFrequency,
                         List<String> consequenceType) {
        this.listOfDiseases = listOfDiseases;
        this.populationFrequencySpecies = populationFrequencySpecies;
        this.populationFrequency = populationFrequency;
        this.consequenceType = consequenceType;
    }

    public List<String> getListOfDiseases() {
        return listOfDiseases;
    }

    public VariantFilter setListOfDiseases(List<String> listOfDiseases) {
        this.listOfDiseases = listOfDiseases;
        return this;
    }

    public List<String> getPopulationFrequencySpecies() {
        return populationFrequencySpecies;
    }

    public double getPopulationFrequency() {
        return populationFrequency;
    }

    public VariantFilter setPopulationFrequencySpecies(List<String> populationFrequencySpecies, double populationFrequency) {
        if (populationFrequency <= 0 || populationFrequency >= 1) {
            throw new IllegalArgumentException("populationFrequency must be a value between 0 and 1");
        }
        this.populationFrequencySpecies = populationFrequencySpecies;
        this.populationFrequency = populationFrequency;
        return this;
    }

    public List<String> getConsequenceType() {
        return consequenceType;
    }

    public VariantFilter setConsequenceType(List<String> consequenceType) {
        this.consequenceType = consequenceType;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantFilter{");
        sb.append("listOfDiseases=").append(listOfDiseases);
        sb.append(", populationFrequencySpecies=").append(populationFrequencySpecies);
        sb.append(", populationFrequency=").append(populationFrequency);
        sb.append(", consequenceType=").append(consequenceType);
        sb.append('}');
        return sb.toString();
    }
}
