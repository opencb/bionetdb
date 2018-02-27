package org.opencb.bionetdb.core;

import htsjdk.variant.variantcontext.VariantContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.NodeIterator;
import org.opencb.bionetdb.core.api.PathIterator;
import org.opencb.bionetdb.core.api.RowIterator;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.api.query.NodeQueryParam;
import org.opencb.bionetdb.core.api.query.PathQuery;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.neo4j.Neo4JVariantLoader;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.NetworkManager;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by joaquin on 1/29/18.
 */
public class BioNetDBManager {

    private String database;
    private BioNetDBConfiguration bioNetDBConfiguration;

    private NetworkDBAdaptor networkDBAdaptor;
    private Logger logger;

    private Map<String, Long> idToUidMap;

    private static final int VARIANT_BATCH_SIZE = 10000;
    private static final int QUERY_MAX_RESULTS = 50000;

    public BioNetDBManager(String database, BioNetDBConfiguration bioNetDBConfiguration) throws BioNetDBException {
        this.database = database;
        this.bioNetDBConfiguration = bioNetDBConfiguration;
        networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration, true);

        idToUidMap = new HashMap<>();

        logger = LoggerFactory.getLogger(BioNetDBManager.class);
    }

    public void loadBiopax(java.nio.file.Path path) throws IOException, BioNetDBException {
        // Parse a BioPax file and get the network
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        logger.info("Parsing BioPax file {}...", path);
        long startTime = System.currentTimeMillis();
        Network network = bioPaxParser.parse(path);
        long stopTime = System.currentTimeMillis();
        logger.info("Done. The file '{}' has been parsed in {} seconds.", path, (stopTime - startTime) / 1000);

        // Inserting the network into the database
        logger.info("Inserting data...");
        startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, QueryOptions.empty());
        stopTime = System.currentTimeMillis();
        logger.info("Done. Data insertion took " + (stopTime - startTime) / 1000 + " seconds.");
    }

    public void loadVcf(java.nio.file.Path path) throws BioNetDBException {
        // VCF loader
        Neo4JVariantLoader neo4JVariantLoader = new Neo4JVariantLoader((Neo4JNetworkDBAdaptor) networkDBAdaptor);
        neo4JVariantLoader.loadVCFFile(path);
    }

    public void annotate() {

    }

    public void annotateGenes(Query query, QueryOptions queryOptions) {
    }

    public void annotateVariants(Query query, QueryOptions queryOptions) {
    }

    public void annotateProtein() throws BioNetDBException, IOException {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
        ProteinClient proteinClient = cellBaseClient.getProteinClient();

        networkDBAdaptor.annotateProtein(proteinClient);
    }

    /*
        long uidCounter = getUidCounter();
        long maxUid = 0;

        Session session = this.driver.session();

        // First, insert Neo4J nodes
        for (Node node: network.getNodes()) {
            long uid = uidCounter + node.getUid();
            if (uid > maxUid) {
                maxUid = uid;
            }
            session.writeTransaction(tx -> {
                node.setUid(uid);
                addNode(tx, node);
                return 1;
            });
        }

        // Second, insert Neo4J relationships
        for (Relation relation: network.getRelations()) {
            long uid = uidCounter + relation.getUid();
            if (uid > maxUid) {
                maxUid = uid;
            }
            session.writeTransaction(tx -> {
                relation.setUid(uid);
                relation.setOrigUid(relation.getOrigUid() + uidCounter);
                relation.setDestUid(relation.getDestUid() + uidCounter);
                addRelation(tx, relation);
                return 1;
            });
        }

        setUidCounter(maxUid + 1);

        session.close();
     */

    //=========================================================================
    // S I M P L E     Q U E R I E S: NODES, PATHS, NETWORK
    //=========================================================================

    //-------------------------------------------------------------------------
    // N O D E S
    //-------------------------------------------------------------------------

    public QueryResult<Node> getNode(long uid) throws BioNetDBException {
        NodeQuery query = new NodeQuery();
        query.put(NodeQueryParam.UID.key(), uid);
        query.put(NodeQueryParam.OUTPUT.key(), "node");
        return nodeQuery(query, QueryOptions.empty());
    }

    public QueryResult<Node> getNode(String id) throws BioNetDBException {
        NodeQuery query = new NodeQuery();
        query.put(NodeQueryParam.ID.key(), id);
        query.put(NodeQueryParam.OUTPUT.key(), "node");
        return nodeQuery(query, QueryOptions.empty());
    }

    public QueryResult<Node> nodeQuery(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        NodeIterator nodeIterator = nodeIterator(query, queryOptions);
        return getQueryResult(nodeIterator);
    }

    public QueryResult<Node> nodeQuery(String cypher) throws BioNetDBException {
        NodeIterator nodeIterator = nodeIterator(cypher);
        return getQueryResult(nodeIterator);
    }

    public NodeIterator nodeIterator(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        return networkDBAdaptor.nodeIterator(query, queryOptions);
    }

    public NodeIterator nodeIterator(String cypher) throws BioNetDBException {
        return networkDBAdaptor.nodeIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // T A B L E S
    //   - a table is a list of rows
    //   - a row is a list of strings
    //-------------------------------------------------------------------------

    public QueryResult<List<Object>> table(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        RowIterator rowIterator = rowIterator(query, queryOptions);
        return getQueryResult(rowIterator);
    }

    public QueryResult<List<Object>> table(String cypher) throws BioNetDBException {
        RowIterator rowIterator = rowIterator(cypher);
        return getQueryResult(rowIterator);
    }

    public RowIterator rowIterator(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        return networkDBAdaptor.rowIterator(query, queryOptions);
    }

    public RowIterator rowIterator(String cypher) throws BioNetDBException {
        return networkDBAdaptor.rowIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // P A T H S
    //-------------------------------------------------------------------------

    public QueryResult<org.opencb.bionetdb.core.network.Path> pathQuery(PathQuery pathQuery, QueryOptions queryOptions)
            throws BioNetDBException {
        PathIterator pathIterator = pathIterator(pathQuery, queryOptions);
        return getQueryResult(pathIterator);
    }

    public QueryResult<org.opencb.bionetdb.core.network.Path> pathQuery(String cypher) throws BioNetDBException {
        PathIterator pathIterator = pathIterator(cypher);
        return getQueryResult(pathIterator);
    }

    public PathIterator pathIterator(PathQuery pathQuery, QueryOptions queryOptions) throws BioNetDBException {
        return networkDBAdaptor.pathIterator(pathQuery, queryOptions);
    }

    public PathIterator pathIterator(String cypher) throws BioNetDBException {
        return networkDBAdaptor.pathIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // N E T W O R K S
    //-------------------------------------------------------------------------

    public QueryResult<Network> networkQuery(List<NodeQuery> nodeQueries, QueryOptions queryOptions)
            throws BioNetDBException {
        return networkDBAdaptor.networkQuery(nodeQueries, queryOptions);
    }

    public QueryResult<Network> networkQueryByPaths(List<PathQuery> pathQueries, QueryOptions queryOptions)
            throws BioNetDBException {
       return networkDBAdaptor.networkQueryByPaths(pathQueries, queryOptions);
    }

    public QueryResult<Network> networkQuery(String cypher) throws BioNetDBException {
        return networkDBAdaptor.networkQuery(cypher);
    }

    //=========================================================================
    // A N A L Y S I S
    //=========================================================================

    public QueryResult getSummaryStats(Query query, QueryOptions queryOptions) throws BioNetDBException {
        return null;
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private List<Variant> convert(List<VariantContext> variantContexts, VariantContextToVariantConverter converter) {
        // Iterate over variant context and convert to variant
        List<Variant> variants = new ArrayList<>(variantContexts.size());
        for (VariantContext variantContext: variantContexts) {
            Variant variant = converter.convert(variantContext);
            variants.add(variant);
        }
        return variants;
    }


    private void updateNodeUids(Node.Type type, Query query, QueryOptions queryOptions, NetworkManager netManager)
            throws BioNetDBException {
        // Get network nodes
        List<Node> nodes = netManager.getNodes(type);

        for (Node node: nodes) {
            String key = type.name() + ":" + node.getId();
            if (idToUidMap.containsKey(key)) {
                netManager.replaceUid(node.getUid(), idToUidMap.get(key));
            } else {
                QueryResult<Node> vNodes = getNode(node.getId());
                if (vNodes.getResult().size() > 0) {
                    Node n = vNodes.getResult().get(0);
                    netManager.replaceUid(node.getUid(), n.getUid());

                    idToUidMap.put(key, n.getUid());
                }
            }

//            System.out.println("node " + node.getType().name() + ": uid=" + node.getUid() + ", id=" + node.getId() + ", name="
//                    + node.getName());
        }
    }

    private QueryResult<Node> getQueryResult(NodeIterator nodeIterator) {
        List<Node> nodes = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (nodeIterator.hasNext()) {
            if (nodes.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            nodes.add(nodeIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("get", time, nodes.size(), nodes.size(), null, null, nodes);

    }

    private QueryResult<List<Object>> getQueryResult(RowIterator rowIterator) {
        List<List<Object>> rows = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (rowIterator.hasNext()) {
            if (rows.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            rows.add(rowIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("table", time, rows.size(), rows.size(), null, null, rows);
    }

    public QueryResult<org.opencb.bionetdb.core.network.Path> getQueryResult(PathIterator pathIterator)
            throws BioNetDBException {
        List<org.opencb.bionetdb.core.network.Path> paths = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (pathIterator.hasNext()) {
            if (paths.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            paths.add(pathIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("get", time, paths.size(), paths.size(), null, null, paths);
    }
}
