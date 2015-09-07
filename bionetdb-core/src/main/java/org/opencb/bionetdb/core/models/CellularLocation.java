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
        this.names = new ArrayList<>();
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

    public void setXref(Xref xref) {
        // Adding xref unless it exists
        boolean duplicate = false;
        for (Xref currentXref : this.getXrefs()) {
            if(xref.getSource().equals(currentXref.getSource()) &&
                    xref.getSourceVersion().equals(currentXref.getSourceVersion()) &&
                    xref.getId().equals(currentXref.getId()) &&
                    xref.getIdVersion().equals(currentXref.getIdVersion())) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            this.getXrefs().add(xref);
        }
    }
}
