package org.opencb.bionetdb.core.models;

/**
 * Created by imedina on 10/08/15.
 */
public class Protein extends PhysicalEntity {

    private String uniProtId;

    public Protein() {
        super("", "", "", Type.PROTEIN);
    }

    public Protein(String id, String name, String description) {
        super(id, name, description, Type.PROTEIN);
    }


}
