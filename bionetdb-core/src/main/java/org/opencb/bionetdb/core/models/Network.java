package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 10/08/15.
 */
public class Network {

    private String id;
    private String name;
    private String description;

    private List<PhysicalEntity> physicalEntities;
    private List<Interaction> interactions;

    protected Map<String, Object> attributes;

    public Network() {
        this.id = "";
        this.name = "";
        this.description = "";

        // init rest of attributes
        init();
    }

    public Network(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;

        // init rest of attributes
        init();
    }

    private void init() {
        physicalEntities = new ArrayList<>();
        interactions = new ArrayList<>();

        attributes = new HashMap<>();
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<PhysicalEntity> getPhysicalEntities() {
        return physicalEntities;
    }

    public void setPhysicalEntities(List<PhysicalEntity> physicalEntities) {
        this.physicalEntities = physicalEntities;
    }

    public List<Interaction> getInteractions() {
        return interactions;
    }

    public void setInteractions(List<Interaction> interactions) {
        this.interactions = interactions;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
