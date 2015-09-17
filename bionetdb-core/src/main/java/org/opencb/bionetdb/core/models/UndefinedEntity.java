package org.opencb.bionetdb.core.models;

import java.util.Collections;
import java.util.List;

/**
 * Created by dapregi on 24/08/15.
 */
public class UndefinedEntity extends PhysicalEntity {

    public UndefinedEntity() {
        super("", "", Collections.<String>emptyList(), Type.UNDEFINEDENTITY);
        init();
    }

    public UndefinedEntity(String id, String name, List<String> description) {
        super(id, name, description, Type.UNDEFINEDENTITY);
        init();
    }

    private void init() {
    }
}
