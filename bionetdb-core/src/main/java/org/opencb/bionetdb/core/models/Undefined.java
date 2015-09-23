package org.opencb.bionetdb.core.models;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by dapregi on 24/08/15.
 */
public class Undefined extends PhysicalEntity {

    public Undefined() {
        super("", "", new ArrayList<>(), Type.UNDEFINED);
        init();
    }

    public Undefined(String id, String name, List<String> description) {
        super(id, name, description, Type.UNDEFINED);
        init();
    }

    private void init() {
    }
}
