package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by dapregi on 19/08/15.
 */
public class Reaction extends Interaction {

    private List<String> reactants;
    private List<String> products;
    private boolean reversible;
    private boolean spontaneous;
    private List<Map<String, Object>> stoichiometry;

    private ReactionType reactionType;

    public enum ReactionType {
        REACTION    ("reaction"),
        ASSEMBLY    ("assembly"),
        TRANSPORT   ("transport");

        private final String reactionType;

        ReactionType(String reactionType) {
            this.reactionType = reactionType;
        }
    }

    public Reaction() {
        super("", "", new ArrayList<>(), Type.REACTION);
        this.reactionType = ReactionType.REACTION;
        init();
    }

    public Reaction(ReactionType reactionType) {
        super("", "", new ArrayList<>(), Type.REACTION);
        this.reactionType = ReactionType.REACTION;
        init();
    }

    public Reaction(String id, String name, List<String> description, ReactionType reactionType) {
        super(id, name, description, Type.REACTION);
        this.reactionType = reactionType;
        init();
    }

    private void init() {
        this.reactants = new ArrayList<>();
        this.products = new ArrayList<>();
        this.stoichiometry = new ArrayList<>();
        this.reversible = false;
        this.spontaneous = false;
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

    public boolean isReversible() {
        return reversible;
    }

    public void setReversible(boolean reversible) {
        this.reversible = reversible;
    }

    public boolean isSpontaneous() {
        return spontaneous;
    }

    public void setSpontaneous(boolean spontaneous) {
        this.spontaneous = spontaneous;
    }

    public List<Map<String, Object>> getStoichiometry() {
        return stoichiometry;
    }

    public void setStoichiometry(List<Map<String, Object>> stoichiometry) {
        this.stoichiometry = stoichiometry;
    }

    public ReactionType getReactionType() {
        return reactionType;
    }

    public void setReactionType(ReactionType reactionType) {
        this.reactionType = reactionType;
    }
}
