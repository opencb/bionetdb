package org.opencb.bionetdb.core.models;

import java.util.Collections;

/**
 * Created by imedina on 10/08/15.
 */
public class Reaction extends Interaction {


    public Reaction() {
        super("", "", Collections.<String>emptyList(), "", Type.REACTION);
    }

}
