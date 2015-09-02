package org.opencb.bionetdb.core.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dapregi on 14/08/15.
 */
public class Rna extends PhysicalEntity {

    public Rna() {
        super("", "", "", Type.RNA);
        init();
    }

    public Rna(String id, String name, String description) {
        super(id, name, description, Type.RNA);
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
