package org.opencb.bionetdb.core.models;

public class Relationship {
    protected String id;
    protected String name;

    protected Type type;

    public enum Type {
        REACTION       ("reaction"),
        CATALYSIS      ("catalysis"),
        REGULATION     ("regulation"),
        COLOCALIZATION ("colocalization");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public static boolean isInteraction(Relationship r) {
        switch (r.type) {
            case REACTION:
            case CATALYSIS:
            case REGULATION:
            case COLOCALIZATION:
                return true;
            default:
                return false;
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }
}
