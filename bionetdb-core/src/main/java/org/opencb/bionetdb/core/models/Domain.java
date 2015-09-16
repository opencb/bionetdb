package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dapregi on 16/09/15.
 */
public class Domain {

    protected String name;
    protected String description;
    protected List<Xref> xrefs;
    protected List<Ontology> ontologies;

    public Domain() {
        init();
    }
    
    private void init() {
        this.name = "";
        this.description = "";
        this.ontologies = new ArrayList<>();
        this.xrefs = new ArrayList<>();
    }
}
