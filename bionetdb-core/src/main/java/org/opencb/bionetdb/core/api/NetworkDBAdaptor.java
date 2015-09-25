package org.opencb.bionetdb.core.api;

import org.opencb.bionetdb.core.exceptions.NetworkDBException;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryParam;
import org.opencb.datastore.core.QueryResult;

import java.util.List;

import static org.opencb.datastore.core.QueryParam.Type.INTEGER;
import static org.opencb.datastore.core.QueryParam.Type.TEXT_ARRAY;

/**
 * Created by imedina on 05/08/15.
 */
public interface NetworkDBAdaptor extends AutoCloseable {

    enum NetworkQueryParams implements QueryParam {
        PE_ID ("xref.id", TEXT_ARRAY, ""),
        PE_DESCRIPTION ("pe.description", TEXT_ARRAY, ""),
        PE_TYPE ("pe.type", TEXT_ARRAY, ""),
        PE_ATTR_EXPR ("pe.attr.expr", TEXT_ARRAY, ""),  // example: "brain:t2>0.3;brain:t4<=0.3"
        PE_ONTOLOGY ("pe.ontology", TEXT_ARRAY, ""),  // example: "go:001234,go:002345"
        PE_CELLOCATION ("pe.cellularLocation", TEXT_ARRAY, ""), // example: "mitochondria,..."
        _JUMPS ("_jumps", INTEGER, ""),
        INT_TYPE ("int.type", TEXT_ARRAY, "");

        NetworkQueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        private final String key;
        private Type type;
        private String description;

        @Override public String key() {
            return key;
        }

        @Override public String description() {
            return description;
        }

        @Override public Type type() {
            return type;
        }
    }


    void insert(Network network, QueryOptions queryOptions) throws NetworkDBException;

    void addXrefs(String nodeID, List<Xref> xrefList) throws NetworkDBException;

    /**
     *
     * @param tissue Tissue of the current expression experiment
     * @param timeSeries Timeseries of the current expression experiment
     * @param myExpression List of expression data to be add in the database
     * @param options Boolean to know if nodes not found in the database have to be created and insert their expression or not
     */
    void addExpressionData(String tissue, String timeSeries, List<Expression> myExpression, QueryOptions options);

    //TODO: To remove
    //public QueryResult getXrefs(String idNode);

    QueryResult get(Query query, QueryOptions queryOptions) throws NetworkDBException;

    QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions);


    QueryResult getSummaryStats(Query query, QueryOptions queryOptions);

}
