package org.opencb.bionetdb.core.neo4j.interpretation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class Threadpool implements Callable<VariantContainer> {

    private XQueryAnalysis xQueryAnalysis;

    private Map<String, List<String>> genotypes;
    private String gene;
    private List<String> listOfDiseases;
    private List<String> populationFrequencySpecies;
    private double populationFrequency;
    private List<String> consequenceType;
    private OptionsFilter optionsFilter;

    Threadpool(XQueryAnalysis xQueryAnalysis, Map<String, List<String>> genotypes, String gene, List<String> listOfDiseases,
               List<String> populationFrequencySpecies, double populationFrequency, List<String> consequenceType,
               OptionsFilter optionsFilter) {
        this.genotypes = genotypes;
        this.gene = gene;
        this.listOfDiseases = listOfDiseases;
        this.populationFrequencySpecies = populationFrequencySpecies;
        this.populationFrequency = populationFrequency;
        this.consequenceType = consequenceType;
        this.optionsFilter = optionsFilter;
        this.xQueryAnalysis = xQueryAnalysis;
    }

    public VariantContainer call() {
        return xQueryAnalysis.xQueryCall(genotypes, Collections.singletonList(gene), listOfDiseases, populationFrequencySpecies,
                populationFrequency, consequenceType, optionsFilter);
    }
}
