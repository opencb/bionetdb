package org.opencb.bionetdb.core.models;

/**
 * Created by imedina on 10/08/15.
 */
public class Protein extends PhysicalEntity {

    private boolean peptide;

    public Protein() {
        super("", "", "", Type.PROTEIN);
        init();
    }

    public Protein(String id, String name, String description) {
        super(id, name, description, Type.PROTEIN);
        init();
    }

    private void init() {
        this.peptide = false;
    }

    public boolean isPeptide() {
        return peptide;
    }

    public void setPeptide(boolean peptide) {
        this.peptide = peptide;
    }
}
