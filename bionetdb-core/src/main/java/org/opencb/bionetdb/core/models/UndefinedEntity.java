package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 24/08/15.
 */
public class UndefinedEntity extends PhysicalEntity {

    public UndefinedEntity() {
        super("", "", "", Type.UNDEFINEDENTITY);
        init();
    }

    public UndefinedEntity(String id, String name, String description) {
        super(id, name, description, Type.UNDEFINEDENTITY);
        init();
    }

    private void init() {
    }
}
