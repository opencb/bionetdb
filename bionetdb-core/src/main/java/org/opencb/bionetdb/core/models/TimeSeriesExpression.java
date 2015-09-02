package org.opencb.bionetdb.core.models;


/**
 * Created by pfurio on 2/9/15.
 */
public class TimeSeriesExpression {

    protected String id;
    protected double expression;
    protected double pvalue;
    protected double odds;
    protected UpRegulatedParams upregulated;

    public enum UpRegulatedParams {
        UP (1),
        DOWN (0),
        UNDEFINED (-1);

        private final int upregulated;

        UpRegulatedParams (int upregulated) { this.upregulated = upregulated; }
        public int upregulated () { return upregulated; }
    }

    public String getId() {
        return id;
    }

    public double getExpression() {
        return expression;
    }

    public double getPvalue() {
        return pvalue;
    }

    public double getOdds() {
        return odds;
    }

    public UpRegulatedParams getUpregulated() {
        return upregulated;
    }

    public TimeSeriesExpression(String id, double expression) {
        this.id = id;
        this.expression = expression;
        this.pvalue = -1;
        this.odds = -1;
        this.upregulated = UpRegulatedParams.UNDEFINED;
    }

    public TimeSeriesExpression(String id, double expression, double pvalue, double odds, boolean upregulated) {
        this.id = id;
        this.expression = expression;
        this.pvalue = pvalue;
        this.odds = odds;
        if (upregulated == true)
            this.upregulated = UpRegulatedParams.UP;
        else
            this.upregulated = UpRegulatedParams.DOWN;
    }
}
