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

//    private List<Network> children;

    protected Map<String, Object> attributes;

    public Network() {
    }

    public Network(String id, String name, String description) {
        this.description = description;
        this.id = id;
        this.name = name;

        // init rest of attributes
        init();
    }

    private void init() {
        physicalEntities = new ArrayList<>();
        interactions = new ArrayList<>();

        attributes = new HashMap<>();
    }
}
