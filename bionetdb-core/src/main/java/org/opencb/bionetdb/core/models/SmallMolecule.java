package org.opencb.bionetdb.core.models;

import java.util.Collections;
import java.util.List;

/**
 * Created by dapregi on 14/08/15.
 */
public class SmallMolecule extends PhysicalEntity {

    public SmallMolecule() {
        super("", "", Collections.<String>emptyList(), Type.SMALLMOLECULE);
        init();
    }

    public SmallMolecule(String id, String name, List<String> description) {
        super(id, name, description, Type.SMALLMOLECULE);
        init();
    }

    private void init() {
    }

}
