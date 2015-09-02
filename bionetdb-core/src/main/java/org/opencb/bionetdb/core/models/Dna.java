package org.opencb.bionetdb.core.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dapregi on 12/08/15.
 */
public class Dna extends PhysicalEntity {

    public Dna() {
        super("", "", "", Type.DNA);
        init();
    }

    public Dna(String id, String name, String description) {
        super(id, name, description, Type.DNA);
        init();
    }

    protected Map<String, TissueExpression> expression;

    public void setExpression(Map<String, TissueExpression> expression) {
        this.expression = expression;
    }

    public Map<String, TissueExpression> getExpression() {

        return expression;
    }

    private void init() {
        this.expression = new HashMap<>();

    }
}
