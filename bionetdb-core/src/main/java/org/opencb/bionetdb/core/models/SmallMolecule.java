package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 14/08/15.
 */
public class SmallMolecule extends PhysicalEntity {

    private String chebiId;

    public SmallMolecule() {
        super("", "", "", Type.SMALLMOLECULE);
        init();
    }

    public SmallMolecule(String id, String name, String description) {
        super(id, name, description, Type.SMALLMOLECULE);
        init();
    }

    private void init() {
        this.chebiId = new String();
    }

    public String getChebiId() {
        return chebiId;
    }

    public void setChebiId(String chebiId) {
        this.chebiId = chebiId;
    }
}
