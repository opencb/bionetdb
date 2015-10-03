package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 24/08/15.
 */
public class Xref {

    private String source;
    private String sourceVersion;
    private String id;
    private String idVersion;

    public Xref() {
        init();
    }

    public Xref(String source, String sourceVersion, String id, String idVersion) {
        init();
        if (source != null) {
            this.source = source.toLowerCase();
        }
        if (sourceVersion != null) {
            this.sourceVersion = sourceVersion;
        }
        if (id != null) {
            this.id = id;
        }
        if (idVersion != null) {
            this.idVersion = idVersion;
        }
    }

    private void init() {
        this.source = "";
        this.sourceVersion = "";
        this.id = "";
        this.idVersion = "";
    }

    public boolean isEqual(Xref that) {
        return this.getId().equals(that.getId()) && this.getSource().equals(that.getSource());
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        if (source != null) {
            this.source = source.toLowerCase();
        }
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(String sourceVersion) {
        if (sourceVersion != null) {
            this.sourceVersion = sourceVersion;
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        if (id != null) {
            this.id = id;
        }
    }

    public String getIdVersion() {
        return idVersion;
    }

    public void setIdVersion(String idVersion) {
        if (idVersion != null) {
            this.idVersion = idVersion;
        }
    }
}
