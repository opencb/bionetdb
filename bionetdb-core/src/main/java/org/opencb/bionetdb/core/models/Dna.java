package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 12/08/15.
 */
public class Dna extends PhysicalEntity {

    public Dna() {
        super("", "", "", Type.DNA);
        init();
    }

    public Dna(String id, String name, String description) {
        super(id, name, description, Type.DNA);
        init();
    }

    private void init() {
    }
}
