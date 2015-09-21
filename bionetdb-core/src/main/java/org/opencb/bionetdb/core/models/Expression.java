package org.opencb.bionetdb.core.models;

/**
 * Created by pfurio on 8/9/15.
 */
public class Expression {

    // TODO: upregulated can be 0 (not upregulated) or 1 (upregulated). Include downregulated?

    private String id;
    private double expression;
    private double pvalue;
    private double odds;
    private int upregulated;

    public Expression() {
        this("");
    }

    public Expression(String id) {
        this.id = id;
        this.expression = -1;
        this.pvalue = -1;
        this.odds = -1;
        this.upregulated = -1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Expression{");
        sb.append("id='").append(id).append('\'');
        sb.append(", expression=").append(expression);
        sb.append(", pvalue=").append(pvalue);
        sb.append(", odds=").append(odds);
        sb.append(", upregulated=").append(upregulated);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getExpression() {
        return expression;
    }

    public void setExpression(double expression) {
        this.expression = expression;
    }

    public double getPvalue() {
        return pvalue;
    }

    public void setPvalue(double pvalue) {
        this.pvalue = pvalue;
    }

    public double getOdds() {
        return odds;
    }

    public void setOdds(double odds) {
        this.odds = odds;
    }

    public int getUpregulated() {
        return upregulated;
    }

    public void setUpregulated(int upregulated) {
        this.upregulated = upregulated;
    }

}
