package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by dapregi on 14/08/15.
 */
public class Complex extends PhysicalEntity {

    private List<String> components;
    private List<Map<String, Object>> stoichiometry;

    public Complex() {
        super("", "", Collections.<String>emptyList(), Type.COMPLEX);
        init();
    }

    public Complex(String id, String name, List<String> description) {
        super(id, name, description, Type.COMPLEX);
        init();
    }

    private void init() {
        this.components = new ArrayList<>();
        this.stoichiometry = new ArrayList<>();
    }

    public List<String> getComponents() {
        return components;
    }

    public void setComponents(List<String> components) {
        this.components = components;
    }

    public List<Map<String, Object>> getStoichiometry() {
        return stoichiometry;
    }

    public void setStoichiometry(List<Map<String, Object>> stoichiometry) {
        this.stoichiometry = stoichiometry;
    }
}
