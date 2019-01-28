package org.opencb.bionetdb.core.api;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.VariantsPair;
import org.opencb.bionetdb.core.api.query.NetworkPathQuery;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.cellbase.client.rest.VariationClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.IOException;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by imedina on 05/08/15.
 */
public interface NetworkDBAdaptor extends AutoCloseable {

    enum NetworkQueryParams implements QueryParam {
        NODE_TYPE("node.type", TEXT_ARRAY, ""),   // This is PHYSICAL_ENTITY, INTERACTION, XREF, ...
        NODE_UID("node.uid", STRING, ""),
        NODE_ID("node.id", TEXT_ARRAY, ""),

        SRC_NODE("src-node", TEXT_ARRAY, ""),
        DEST_NODE("dest-node", TEXT_ARRAY, ""),
        INTERM_NODE("interm-node", TEXT_ARRAY, ""),
        MAX_JUMPS("max-jumps", INTEGER, ""),
        OUTPUT("output", STRING, ""),

        PE_ID ("pe.id", TEXT_ARRAY, ""),
        PE_DESCRIPTION ("pe.description", TEXT_ARRAY, ""),
        REL_TYPE("rel.type", TEXT_ARRAY, ""),
        PE_ATTR_EXPR ("pe.attr.expr", TEXT_ARRAY, ""),  // example: "brain:t2>0.3;brain:t4<=0.3"
        PE_ONTOLOGY ("pe.ontology", TEXT_ARRAY, ""),  // example: "go:001234, go:002345"
        PE_CELLOCATION ("pe.cellularLocation", TEXT_ARRAY, ""), // example: "nucleoplasm,..."
        JUMPS("jumps", INTEGER, ""),
        SCRIPT ("script",  STRING, ""),

        ID("id", TEXT_ARRAY, ""),
        LABEL("label", TEXT_ARRAY, "");

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

    //-------------------------------------------------------------------------
    // I N S E R T     N E T W O R K S
    //-------------------------------------------------------------------------

    void insert(Network network, QueryOptions queryOptions) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // A N N O T A T I O N     M E T H O D s
    //-------------------------------------------------------------------------

    void annotateVariants(NodeQuery query, QueryOptions options, VariationClient variationClient) throws BioNetDBException, IOException;
    void annotateVariants(List<String> variantIds, VariationClient variationClient) throws BioNetDBException, IOException;

    void annotateGenes(NodeQuery query, QueryOptions options, GeneClient geneClient) throws BioNetDBException, IOException;
    void annotateGenes(List<String> geneIds, GeneClient geneClient) throws BioNetDBException, IOException;

    void annotateProteins(NodeQuery query, QueryOptions options, ProteinClient proteinClient) throws BioNetDBException, IOException;
    void annotateProteins(List<String> proteinIds, ProteinClient proteinClient) throws BioNetDBException, IOException;

    //-------------------------------------------------------------------------
    // N O D E S
    //-------------------------------------------------------------------------

    NodeIterator nodeIterator(Query query, QueryOptions queryOptions) throws BioNetDBException;
    NodeIterator nodeIterator(String cypher) throws BioNetDBException;

    QueryResult<Node> nodeQuery(Query query, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Node> nodeQuery(String cypher) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // T A B L E S
    //   - a table is modeled as a list of rows, and
    //   - a row is modeled as a list of Object
    //-------------------------------------------------------------------------

    RowIterator rowIterator(Query query, QueryOptions queryOptions) throws BioNetDBException;
    RowIterator rowIterator(String cypher) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // N E T W O R K     P A T H S
    //   - a network path is modeled as a 'lineal' network, it contains
    //     the origin node and the destionation node
    //-------------------------------------------------------------------------

    NetworkPathIterator networkPathIterator(NetworkPathQuery networkPathQuery, QueryOptions queryOptions) throws BioNetDBException;
    NetworkPathIterator networkPathIterator(String cypher) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // N E T W O R K S
    //-------------------------------------------------------------------------

    QueryResult<Network> networkQuery(List<NodeQuery> nodeQueries, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Network> networkQueryByPaths(List<NetworkPathQuery> pathQueries, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Network> networkQuery(String cypher) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // A N A L Y S I S
    //-------------------------------------------------------------------------

    List<Variant> getMatchingDominantVariants(String child, String father, String mother, QueryOptions options);

    List<Variant> getMatchingRecessiveVariants(String child, String father, String mother, QueryOptions options);

    List<Variant> getMatchingDeNovoVariants(String child, String father, String mother, QueryOptions options);

    List<Variant> getMatchingXLinkedVariants(String child, String father, String mother, QueryOptions options);

    List<VariantsPair> getMatchingVariantsInSameGen(String child, String father, String mother, int limit);

    List<String> getSpecificBurdenTest(List<String> genes);

    //-------------------------------------------------------------------------
    // T E S T S
    //-------------------------------------------------------------------------

    void loadTest();

//    QueryResult<Node> queryNodes(Query query, QueryOptions queryOptions) throws BioNetDBException;
//
//    QueryResult<Network> networkQuery(Query query, QueryOptions queryOptions) throws BioNetDBException;
//    QueryResult<Network> networkQuery(String cypher) throws BioNetDBException;
//
//    NodeIterator nodeIterator(Query query, QueryOptions queryOptions);
//    NodeIterator nodeIterator(String cypher);
//
//    RowIterator rowIterator(Query query, QueryOptions queryOptions);
//    RowIterator rowIterator(String cypher);
//
//    NetworkPathIterator networkPathIterator(NodeQuery srcQuery, Query destQuery, QueryOptions queryOptions);
//    NetworkPathIterator networkPathIterator(String cypher);

    //    void addXrefs(String nodeID, List<Xref> xrefList) throws BioNetDBException;
//
//    void addVariants(List<Variant> variants) throws BioNetDBException;
//
//    /**
//     *
//     * @param tissue Tissue of the current expression experiment
//     * @param timeSeries Timeseries of the current expression experiment
//     * @param myExpression List of expression data to be add in the database
//     * @param options Boolean to know if nodes not found in the database have to be created and insert their expression or not
//     */
//    void addExpressionData(String tissue, String timeSeries, List<Expression> myExpression, QueryOptions options);
//
//    QueryResult getNodes(Query query, QueryOptions queryOptions) throws BioNetDBException;
//
//    QueryResult getNodes(Query queryN, Query queryM, QueryOptions queryOptions) throws BioNetDBException;
//
//    QueryResult getNetwork(Query query, QueryOptions queryOptions) throws BioNetDBException;
//
//    QueryResult getSummaryStats(Query query, QueryOptions queryOptions);
//
//    QueryResult betweenness(Query query);
//
//    QueryResult clusteringCoefficient(Query query);
//
//    QueryResult getAnnotations(Query query, String annotateField);
}
