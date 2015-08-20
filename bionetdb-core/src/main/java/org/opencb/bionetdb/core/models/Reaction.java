package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by dapregi on 19/08/15.
 */
public class Reaction extends Interaction {

    private List<String> reactants;
    private List<String> products;
    private Boolean reversible;
    private Boolean spontaneous;
    private List<Map<String, Object>> stoichiometry;

    private ReactionType reactionType;

    enum ReactionType {
        REACTION    ("reaction"),
        ASSEMBLY    ("assembly"),
        TRANSPORT   ("transport");

        private final String reactionType;

        ReactionType(String reactionType) {
            this.reactionType = reactionType;
        }
    }

    public Reaction() {
        super("", "", "", Type.REACTION);
        this.reactionType = ReactionType.REACTION;
        init();
    }

    public Reaction(String id, String name, String description) {
        super(id, name, description, Type.REACTION);
        this.reactionType = ReactionType.REACTION;
        init();
    }

    public Reaction(String id, String name, ReactionType reactionType, String description) {
        super(id, name, description, Type.REACTION);
        this.reactionType = reactionType;
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
