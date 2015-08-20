package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by dapregi on 19/08/15.
 */
@Deprecated
public class Assembly extends Interaction {

    private List<String> reactants;
    private List<String> products;
    private Boolean reversible;
    private Boolean spontaneous;
    private List<Map<String, Object>> stoichiometry;

    public Assembly() {
        super("", "", "", Type.ASSEMBLY);
        init();
    }

    public Assembly(String id, String name, String description) {
        super(id, name, description, Type.ASSEMBLY);
        init();
    }

    private void init() {
        this.reactants = new ArrayList<>();
        this.products = new ArrayList<>();
        this.stoichiometry = new ArrayList<>();
        this.reversible = null;
        this.spontaneous = null;
    }

    public List<String> getReactants() {
        return reactants;
    }

    public void setReactants(List<String> reactants) {
        this.reactants = reactants;
    }

    public List<String> getProducts() {
        return products;
    }

    public void setProducts(List<String> products) {
        this.products = products;
    }

    public List<Map<String, Object>> getStoichiometry() {
        return stoichiometry;
    }

    public void setStoichiometry(List<Map<String, Object>> stoichiometry) {
        this.stoichiometry = stoichiometry;
    }

    public Boolean getReversible() {
        return reversible;
    }

    public void setReversible(Boolean reversible) {
        this.reversible = reversible;
    }

    public Boolean getSpontaneous() {
        return spontaneous;
    }

    public void setSpontaneous(Boolean spontaneous) {
        this.spontaneous = spontaneous;
    }
}
