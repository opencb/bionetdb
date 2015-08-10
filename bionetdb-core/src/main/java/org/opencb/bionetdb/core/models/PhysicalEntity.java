package org.opencb.bionetdb.core.models;

import java.util.Map;

/**
 * Created by imedina on 10/08/15.
 */
public class PhysicalEntity {

    protected String id;
    protected String name;

    protected Type type;

    protected Map<String, Object> attributes;

    // TODO think about his!
    protected Display display;

    enum Type {
        PROTEIN ("protein"),
        GENE    ("gene");


        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public PhysicalEntity() {

    }

    public PhysicalEntity(String id, String name, Type type) {
        this.id = id;
        this.name = name;
        this.type = type;
    }






    class Display {

        private int x;
        private int y;
        private int z;


    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Display getDisplay() {
        return display;
    }

    public void setDisplay(Display display) {
        this.display = display;
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
