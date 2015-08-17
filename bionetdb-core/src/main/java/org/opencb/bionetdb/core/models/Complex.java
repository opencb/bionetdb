package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dapregi on 14/08/15.
 */
public class Complex extends PhysicalEntity {

    private List<String> components;
    private Map<String, Float> stoichiometry;

    public Complex() {
        super("", "", "", Type.COMPLEX);
        init();
    }

    public Complex(String id, String name, String description) {
        super(id, name, description, Type.COMPLEX);
        init();
    }

    private void init() {
        this.components = new ArrayList<>();
        this.stoichiometry = new HashMap<>();
    }

    public List<String> getComponents() {
        return components;
    }

    public void setComponents(List<String> components) {
        this.components = components;
    }

    public Map<String, Float> getStoichiometry() {
        return stoichiometry;
    }

    public void setStoichiometry(Map<String, Float> stoichiometry) {
        this.stoichiometry = stoichiometry;
    }
}
