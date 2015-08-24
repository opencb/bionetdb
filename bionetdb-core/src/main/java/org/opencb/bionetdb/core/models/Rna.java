package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 14/08/15.
 */
public class Rna extends PhysicalEntity {

    private String ensemblId;

    public Rna() {
        super("", "", "", Type.RNA);
        init();
    }

    public Rna(String id, String name, String description) {
        super(id, name, description, Type.RNA);
        init();
    }

    private void init() {
        this.ensemblId = new String();
    }

    public String getEnsemblId() {
        return ensemblId;
    }

    public void setEnsemblId(String ensemblId) {
        this.ensemblId = ensemblId;
    }
}
