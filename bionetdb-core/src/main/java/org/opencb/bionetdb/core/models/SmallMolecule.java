package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 14/08/15.
 */
public class SmallMolecule extends PhysicalEntity {

    public SmallMolecule() {
        super("", "", "", Type.SMALLMOLECULE);
        init();
    }

    public SmallMolecule(String id, String name, String description) {
        super(id, name, description, Type.SMALLMOLECULE);
        init();
    }

    private void init() {
    }

}
