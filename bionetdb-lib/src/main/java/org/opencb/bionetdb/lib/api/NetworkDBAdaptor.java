package org.opencb.bionetdb.lib.api;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.lib.api.iterators.NetworkPathIterator;
import org.opencb.bionetdb.lib.api.iterators.NodeIterator;
import org.opencb.bionetdb.lib.api.iterators.RowIterator;
import org.opencb.bionetdb.lib.api.iterators.VariantIterator;
import org.opencb.bionetdb.lib.api.query.NetworkPathQuery;
import org.opencb.bionetdb.lib.api.query.NodeQuery;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;

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
    // I N D E X
    //-------------------------------------------------------------------------

    void index();
    void close();

    //-------------------------------------------------------------------------
    // I N S E R T     N E T W O R K S
    //-------------------------------------------------------------------------

    void insert(Network network, QueryOptions queryOptions) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // A N N O T A T I O N     M E T H O D s
    //-------------------------------------------------------------------------

//    void annotateVariants(NodeQuery query, QueryOptions options, VariantClient variantClient) throws BioNetDBException, IOException;
//    void annotateVariants(List<String> variantIds, VariantClient variantClient) throws BioNetDBException, IOException;
//
//    void annotateGenes(NodeQuery query, QueryOptions options, GeneClient geneClient) throws BioNetDBException, IOException;
//    void annotateGenes(List<String> geneIds, GeneClient geneClient) throws BioNetDBException, IOException;
//
//    void annotateProteins(NodeQuery query, QueryOptions options, ProteinClient proteinClient) throws BioNetDBException, IOException;
//    void annotateProteins(List<String> proteinIds, ProteinClient proteinClient) throws BioNetDBException, IOException;

    //========================================================================
    // N E T W O R K
    //========================================================================

    //-------------------------------------------------------------------------
    // N O D E S
    //-------------------------------------------------------------------------

    NodeIterator nodeIterator(Query query, QueryOptions queryOptions) throws BioNetDBException;
    NodeIterator nodeIterator(String cypher) throws BioNetDBException;

    QueryResult<Node> nodeQuery(Query query, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Node> nodeQuery(String cypher) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // T A B L E S / R O W S
    //   - a table is modeled as a list of rows, and
    //   - a row is modeled as a list of Object
    //-------------------------------------------------------------------------

    RowIterator rowIterator(Query query, QueryOptions queryOptions) throws BioNetDBException;
    RowIterator rowIterator(String cypher) throws BioNetDBException;

    QueryResult<List<Object>> rowQuery(Query query, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<List<Object>> rowQuery(String cypher) throws BioNetDBException;


    //-------------------------------------------------------------------------
    // N E T W O R K     P A T H S
    //   - a network path is modeled as a 'lineal' network, it contains
    //     the origin node and the destionation node
    //-------------------------------------------------------------------------

    NetworkPathIterator networkPathIterator(NetworkPathQuery networkPathQuery, QueryOptions queryOptions) throws BioNetDBException;
    NetworkPathIterator networkPathIterator(String cypher) throws BioNetDBException;

    QueryResult<NetworkPath> networkPathQuery(Query query, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<NetworkPath> networkPathQuery(String cypher) throws BioNetDBException;

    //-------------------------------------------------------------------------
    // N E T W O R K S
    //-------------------------------------------------------------------------

    QueryResult<Network> networkQuery(List<NodeQuery> nodeQueries, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Network> networkQueryByPaths(List<NetworkPathQuery> pathQueries, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Network> networkQuery(String cypher) throws BioNetDBException;

    //========================================================================
    // V A R I A N T
    //========================================================================

    VariantIterator variantIterator(Query query, QueryOptions queryOptions) throws BioNetDBException;
    VariantIterator variantIterator(String cypher) throws BioNetDBException;

    QueryResult<Variant> variantQuery(Query query, QueryOptions queryOptions) throws BioNetDBException;
    QueryResult<Variant> variantQuery(String cypher) throws BioNetDBException;

    //========================================================================
    // I N T E R P R E T A T I O N     A N A L Y S I S
    //========================================================================

    QueryResult<Variant> proteinNetworkInterpretationAnalysis(boolean complexOrReaction, Query query) throws BioNetDBException;
}
