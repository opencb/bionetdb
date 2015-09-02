package org.opencb.bionetdb.core.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pfurio on 2/9/15.
 */
public class TissueExpression {

    public TissueExpression(String id, String description) {
        this.id = id;
        this.description = description;
        timeSeries= new HashMap<>();
    }

    protected String id;
    protected String description;
    protected Map<String, TimeSeriesExpression> timeSeries;

    public void settimeSeries(Map<String, TimeSeriesExpression> timeSeries) {
        this.timeSeries= timeSeries;
    }

    public void addValue(String time, TimeSeriesExpression timeSeries) {
        this.timeSeries.put(time, timeSeries);
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, TimeSeriesExpression> gettimeSeries() {
        return timeSeries;
    }
}
