package org.opencb.bionetdb.core.models;

/**
 * Created by imedina on 10/08/15.
 */
public class Protein extends PhysicalEntity {

    private String uniProtId;

    public Protein() {
        super("", "", "", Type.PROTEIN);
        init();
    }

    public Protein(String id, String name, String description) {
        super(id, name, description, Type.PROTEIN);
        init();
    }

    private void init() {
        this.uniProtId = new String();
    }


    public String getUniProtId() {
        return uniProtId;
    }

    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }

}
