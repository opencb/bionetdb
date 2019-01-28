package org.opencb.bionetdb.core.neo4j.interpretation;

import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;

import java.util.List;

public class GeneFilter {

    private List<String> genes;
    private List<DiseasePanel> panels;
    private List<String> diseases;

    public GeneFilter() {}

    public GeneFilter(List<DiseasePanel> panels) {
        this.panels = panels;
    }

    public GeneFilter(List<String> genes, List<DiseasePanel> panels, List<String> diseases) {
        this.genes = genes;
        this.panels = panels;
        this.diseases = diseases;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GeneFilter{");
        sb.append("genes=").append(genes);
        sb.append(", panels=").append(panels);
        sb.append(", diseases=").append(diseases);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getGenes() {
        return genes;
    }

    public GeneFilter setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public List<DiseasePanel> getPanels() {
        return panels;
    }

    public GeneFilter setPanels(List<DiseasePanel> panels) {
        this.panels = panels;
        return this;
    }

    public List<String> getDiseases() {
        return diseases;
    }

    public GeneFilter setDiseases(List<String> diseases) {
        this.diseases = diseases;
        return this;
    }
}
