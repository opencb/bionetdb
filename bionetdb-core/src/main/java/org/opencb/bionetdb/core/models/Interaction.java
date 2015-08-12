package org.opencb.bionetdb.core.models;

import java.util.List;

/**
 * Created by imedina on 10/08/15.
 */
public class Interaction {

    protected String id;
    protected String name;
    protected String description;

    protected List<String> miTerms;
    protected Type type;

    enum Type {
        REACTION    ("reaction"),
        CATALYSIS   ("catalysis"),
        REGULATION  ("regulation"),
        ASSEMBLY    ("assembly"),
        TRANSPORT   ("transport");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public Interaction() {
    }

    public Interaction(String description, String id, List<String> miTerms, String name, Type type) {
        this.description = description;
        this.id = id;
        this.miTerms = miTerms;
        this.name = name;
        this.type = type;
    }

}
