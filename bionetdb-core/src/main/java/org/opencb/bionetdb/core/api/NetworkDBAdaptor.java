package org.opencb.bionetdb.core.api;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER;
import static org.opencb.commons.datastore.core.QueryParam.Type.STRING;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

/**
 * Created by imedina on 05/08/15.
 */
public interface NetworkDBAdaptor extends AutoCloseable {

    enum NetworkQueryParams implements QueryParam {
        NODE_TYPE("node.type", TEXT_ARRAY, ""),   // This is PHYSICAL_ENTITY, INTERACTION, XREF, ...
        PE_ID ("pe.id", TEXT_ARRAY, ""),
        PE_DESCRIPTION ("pe.description", TEXT_ARRAY, ""),
        REL_TYPE("rel.type", TEXT_ARRAY, ""),
        PE_ATTR_EXPR ("pe.attr.expr", TEXT_ARRAY, ""),  // example: "brain:t2>0.3;brain:t4<=0.3"
        PE_ONTOLOGY ("pe.ontology", TEXT_ARRAY, ""),  // example: "go:001234, go:002345"
        PE_CELLOCATION ("pe.cellularLocation", TEXT_ARRAY, ""), // example: "nucleoplasm,..."
        JUMPS("jumps", INTEGER, ""),
        SCRIPT ("script",  STRING, "");

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


    void insert(Network network, QueryOptions queryOptions) throws BioNetDBException;

    void addXrefs(String nodeID, List<Xref> xrefList) throws BioNetDBException;

    void addVariants(List<Variant> variants) throws BioNetDBException;

    /**
     *
     * @param tissue Tissue of the current expression experiment
     * @param timeSeries Timeseries of the current expression experiment
     * @param myExpression List of expression data to be add in the database
     * @param options Boolean to know if nodes not found in the database have to be created and insert their expression or not
     */
    void addExpressionData(String tissue, String timeSeries, List<Expression> myExpression, QueryOptions options);

    QueryResult getNodes(Query query, QueryOptions queryOptions) throws BioNetDBException;

    QueryResult getNodes(Query queryN, Query queryM, QueryOptions queryOptions) throws BioNetDBException;

    QueryResult getNetwork(Query query, QueryOptions queryOptions) throws BioNetDBException;

    QueryResult getSummaryStats(Query query, QueryOptions queryOptions);

    QueryResult betweenness(Query query);

    QueryResult clusteringCoefficient(Query query);

    QueryResult getAnnotations(Query query, String annotateField);

}
