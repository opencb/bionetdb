package org.opencb.bionetdb.core.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dapregi on 14/08/15.
 */
public class Rna extends PhysicalEntity {

    protected RnaType rnaType;

    public enum RnaType {
        MRNA   ("mRNA"),
        RRNA   ("rRNA"),
        TRNA   ("tRNA"),
        LNCRNA ("lncRNA"),
        MIRNA  ("miRNA"),
        SNRNA  ("snRNA");

        private final String rnaType;

        RnaType(String rnaType) {
            this.rnaType = rnaType;
        }
    }

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

    public RnaType getRnaType() {
        return rnaType;
    }

    public void setRnaType(RnaType rnaType) {
        this.rnaType = rnaType;
    }
}
