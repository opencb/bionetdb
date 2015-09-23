package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 10/08/15.
 */
public class Protein extends PhysicalEntity {

    private boolean peptide;
    private List<Domain> domains;

    public Protein() {
        super("", "", new ArrayList<>(), Type.PROTEIN);
        init();
    }

    public Protein(String id, String name, List<String> description) {
        super(id, name, description, Type.PROTEIN);
        init();
    }

    private void init() {
        this.peptide = false;
        this.domains = new ArrayList<>();
    }

    public boolean isPeptide() {
        return peptide;
    }

    public void setPeptide(boolean peptide) {
        this.peptide = peptide;
    }

    public List<Domain> getDomains() {
        return domains;
    }

    public void setDomains(List<Domain> domains) {
        this.domains = domains;
    }

}
