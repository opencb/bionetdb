package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 14/08/15.
 */
public class Rna extends PhysicalEntity {

    public Rna() {
        super("", "", "", Type.RNA);
    }

    public Rna(String id, String name, String description) {
        super(id, name, description, Type.RNA);
    }

}
