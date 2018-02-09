package org.opencb.bionetdb.core.api.query;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

public class NodeQueryParam implements QueryParam {
    private static final List<NodeQueryParam> VALUES = new ArrayList<>();

    public static final String UID_DESCR = "List of node unique ids";
    public static final NodeQueryParam UID = new NodeQueryParam("uid", TEXT_ARRAY, UID_DESCR);

    public static final String ID_DESCR = "List of node ids";
    public static final NodeQueryParam ID = new NodeQueryParam("id", TEXT_ARRAY, ID_DESCR);

    public static final String OUTPUT_DESCR = "Output can be 'node' or a list of specific attribute names separated by commas";
    public static final NodeQueryParam OUTPUT = new NodeQueryParam("output", TEXT_ARRAY, ID_DESCR);

    public static final String FILTER_DESCR = "List of generic attribute filters, three-argument format: attribute name, operation and "
            + "value, e.g.: chromosme='3'";
    public static final NodeQueryParam FILTER = new NodeQueryParam("filters", TEXT_ARRAY, FILTER_DESCR);

    protected NodeQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;
        VALUES.add(this);
    }

    private final String key;
    private final Type type;
    private final String description;

    @Override
    public String key() {
        return key;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return key() + " [" + type() + "] : " + description();
    }

    public static List<NodeQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}
