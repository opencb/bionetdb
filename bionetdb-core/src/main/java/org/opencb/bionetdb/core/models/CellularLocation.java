package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dapregi on 3/09/15.
 */
public class CellularLocation {

    private List<String> names;
    private List<Xref> xrefs;

    public CellularLocation() {
        this.names = new ArrayList<>();;
        this.xrefs = new ArrayList<>();
    }

    public CellularLocation(List<String> names, List<Xref> xrefs) {
        this.names = names;
        this.xrefs = xrefs;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

    public List<Xref> getXrefs() {
        return xrefs;
    }

    public void setXrefs(List<Xref> xrefs) {
        this.xrefs = xrefs;
    }
}
