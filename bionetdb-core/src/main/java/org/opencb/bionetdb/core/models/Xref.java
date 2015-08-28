package org.opencb.bionetdb.core.models;

/**
 * Created by dapregi on 24/08/15.
 */
public class Xref {

    private String db;
    private String dbVersion;
    private String id;
    private String idVersion;

    public Xref() {
        this.db = "";
        this.dbVersion = "";
        this.id = "";
        this.idVersion = "";
    }

    public Xref(String db, String dbVersion, String id, String idVersion) {
        this.db = db;
        this.dbVersion = dbVersion;
        this.id = id;
        this.idVersion = idVersion;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        if (db != null) {
            this.db = db.toLowerCase();
        }
    }

    public String getDbVersion() {
        return dbVersion;
    }

    public void setDbVersion(String dbVersion) {
        if (dbVersion != null) {
            this.dbVersion = dbVersion;
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
