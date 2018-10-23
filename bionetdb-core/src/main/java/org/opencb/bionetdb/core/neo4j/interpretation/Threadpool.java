package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.BioNetDbManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class Threadpool implements Callable<List<List<Variant>>> {

    private XQueryAnalysis xq = BioNetDbManager.xQueryAnalysis;

    private Map<String, List<String>> genotypes;
    private String gene;
    private List<String> listOfDiseases;
    private List<String> populationFrequencySpecies;
    private double populationFrequency;
    private List<String> consequenceType;
    private OptionsFilter optionsFilter;

    Threadpool(Map<String, List<String>> genotypes, String gene, List<String> listOfDiseases, List<String> populationFrequencySpecies,
               double populationFrequency, List<String> consequenceType, OptionsFilter optionsFilter) {
        this.genotypes = genotypes;
        this.gene = gene;
        this.listOfDiseases = listOfDiseases;
        this.populationFrequencySpecies = populationFrequencySpecies;
        this.populationFrequency = populationFrequency;
        this.consequenceType = consequenceType;
        this.optionsFilter = optionsFilter;
    }

    public List<List<Variant>> call() {
        return xq.xQueryCall(genotypes, Collections.singletonList(gene), listOfDiseases, populationFrequencySpecies,
                populationFrequency, consequenceType, optionsFilter);
    }
}
