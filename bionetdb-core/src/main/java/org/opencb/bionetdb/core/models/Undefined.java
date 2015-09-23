package org.opencb.bionetdb.core.models;

import java.util.Collections;
import java.util.List;

/**
 * Created by dapregi on 24/08/15.
 */
public class Undefined extends PhysicalEntity {

    public Undefined() {
        super("", "", Collections.<String>emptyList(), Type.UNDEFINED);
        init();
    }

    public Undefined(String id, String name, List<String> description) {
        super(id, name, description, Type.UNDEFINED);
        init();
    }

    private void init() {
    }
}
