package org.opencb.bionetdb.core;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.Session;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.analysis.InterpretationAnalysis;
import org.opencb.bionetdb.core.analysis.NetworkAnalysis;
import org.opencb.bionetdb.core.analysis.VariantAnalysis;
import org.opencb.bionetdb.core.analysis.interpretation.TieringInterpretationAnalysis;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.iterators.NetworkPathIterator;
import org.opencb.bionetdb.core.api.iterators.NodeIterator;
import org.opencb.bionetdb.core.api.iterators.RowIterator;
import org.opencb.bionetdb.core.api.query.NetworkPathQuery;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.api.query.NodeQueryParam;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.core.models.network.NetworkManager;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.neo4j.Neo4JBioPaxLoader;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JVariantLoader;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.*;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by joaquin on 1/29/18.
 */
public class BioNetDbManager {

    private String database;
    private BioNetDBConfiguration configuration;
    private ClientConfiguration cellbaseClientConfiguration;
    private CellBaseClient cellBaseClient;

    private NetworkDBAdaptor networkDBAdaptor;
    private Logger logger;

    private Map<String, Long> idToUidMap;

    private static final int VARIANT_BATCH_SIZE = 10000;
    private static final int QUERY_MAX_RESULTS = 50000;
    private TieringInterpretationAnalysis tieringInterpretationAnalysis;

    public BioNetDbManager(BioNetDBConfiguration configuration) throws BioNetDBException {
        init(null, configuration);
    }

    public BioNetDbManager(String database, BioNetDBConfiguration configuration) throws BioNetDBException {
        init(database, configuration);
    }

    private void init(String database, BioNetDBConfiguration configuration) throws BioNetDBException {
        // We first create te logger to debug next actions
        logger = LoggerFactory.getLogger(BioNetDbManager.class);

        // We check that the configuration exists and the databases are not empty
        if (configuration == null || configuration.getDatabases() == null || configuration.getDatabases().size() == 0) {
            logger.error("BioNetDB configuration is null or databases are empty");
            throw new BioNetDBException("BioNetDBConfiguration is null or databases are empty");
        }
        this.configuration = configuration;

        // If database parameter is empty then we use take the first database
        if (StringUtils.isNotEmpty(database)) {
            this.database = database;
        } else {
            logger.debug("Empty database parameter: {}, using the first instance of 'databases'", database);
            this.database = this.configuration.getDatabases().get(0).getId();
        }

        // We can now create the default NetworkDBAdaptor
        boolean createIndex = false; // true
        networkDBAdaptor = new Neo4JNetworkDBAdaptor(this.database, this.configuration, createIndex);
        tieringInterpretationAnalysis = new TieringInterpretationAnalysis(((Neo4JNetworkDBAdaptor) this.networkDBAdaptor).getDriver());

        // We create CellBase client
        cellbaseClientConfiguration = new ClientConfiguration();
        cellbaseClientConfiguration.setVersion("v4");
        cellbaseClientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        cellBaseClient = new CellBaseClient(this.configuration.findDatabase(database).getSpecies(), cellbaseClientConfiguration);

        idToUidMap = new HashMap<>();
    }

    //---------------------------------------------
    // A N A L Y S I S
    //---------------------------------------------

    public NetworkAnalysis getNetworkAnalysis() {
        return new NetworkAnalysis(networkDBAdaptor);
    }

    public VariantAnalysis getVariantAnalysis() {
        return new VariantAnalysis(networkDBAdaptor);
    }

    public InterpretationAnalysis getInterpretationAnalysis() {
        return new InterpretationAnalysis(networkDBAdaptor);
    }

    //---------------------------------------------


    public void loadBioPax(java.nio.file.Path path) throws IOException, BioNetDBException {
        loadBioPax(path, null);
    }

    public void loadBioPax(java.nio.file.Path path, Map<String, Set<String>> filters) throws IOException, BioNetDBException {
        // BioPax loader
        Neo4JBioPaxLoader neo4JBioPaxLoader = new Neo4JBioPaxLoader((Neo4JNetworkDBAdaptor) networkDBAdaptor, filters);
        neo4JBioPaxLoader.loadBioPaxFile(path);
    }

    public void loadVcf(java.nio.file.Path path) throws BioNetDBException {
        // VCF loader
        Neo4JVariantLoader neo4JVariantLoader = new Neo4JVariantLoader((Neo4JNetworkDBAdaptor) networkDBAdaptor);
        neo4JVariantLoader.loadVCFFile(path);
    }

    public void loadClinicalAnalysis(Path inputPath) throws IOException {
        Session session = ((Neo4JNetworkDBAdaptor) this.networkDBAdaptor).getDriver().session();

        // Reading file line by line, each line a JSON object (corresponding to a clinical analysis)
        BufferedReader reader = FileUtils.newBufferedReader(inputPath);

        long counter = 0;
        String line = reader.readLine();
        while (line != null) {
            counter++;
            System.out.println("Loading clinical analysis #" + counter + ". Size: " + line.length() + " bytes");

            // Call user defined procedure: loadClinicalAnalysis
            session.run("CALL org.opencb.bionetdb.core.neo4j.loadClinicalAnalysis($caJson)",
                    parameters("caJson", line));

            // Read next line
            line = reader.readLine();
        }
        System.out.println("Loaded " + counter + " clinical analysis");

        reader.close();
    }

    public void importFiles(Path inputPath, Path outputPath, Path neo4jHome) throws BioNetDBException, IOException, InterruptedException {
        // Import
        Neo4JVariantLoader neo4JVariantLoader = new Neo4JVariantLoader((Neo4JNetworkDBAdaptor) networkDBAdaptor);
        neo4JVariantLoader.importFiles(inputPath, outputPath, neo4jHome);
    }

    public void loadClinicalVariant() throws IOException, BioNetDBException {
        Neo4JVariantLoader neo4JVariantLoader = new Neo4JVariantLoader((Neo4JNetworkDBAdaptor) networkDBAdaptor);
        neo4JVariantLoader.loadClinivalVariants(cellBaseClient.getClinicalClient());
    }

    public void annotate() {

    }

    public void annotateGenes(NodeQuery query, QueryOptions options) throws IOException, BioNetDBException {
        GeneClient geneClient = cellBaseClient.getGeneClient();
        networkDBAdaptor.annotateGenes(query, options, geneClient);
    }

    public void annotateGenes(List<String> geneIds) throws IOException, BioNetDBException {
        GeneClient geneClient = cellBaseClient.getGeneClient();
        networkDBAdaptor.annotateGenes(geneIds, geneClient);
    }

    public void annotateVariants(NodeQuery query, QueryOptions options) throws IOException, BioNetDBException {
        VariantClient variationClient = cellBaseClient.getVariantClient();
        networkDBAdaptor.annotateVariants(query, options, variationClient);
    }

    public void annotateVariants(List<String> variantIds) throws IOException, BioNetDBException {
        VariantClient variationClient = cellBaseClient.getVariantClient();
        networkDBAdaptor.annotateVariants(variantIds, variationClient);
    }

    public void annotateProteins(NodeQuery query, QueryOptions options) throws IOException, BioNetDBException {
        ProteinClient proteinClient = cellBaseClient.getProteinClient();
        networkDBAdaptor.annotateProteins(query, options, proteinClient);
    }

    public void annotateProteins(List<String> proteinIds) throws IOException, BioNetDBException {
        ProteinClient proteinClient = cellBaseClient.getProteinClient();
        networkDBAdaptor.annotateProteins(proteinIds, proteinClient);
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

    public void close() throws Exception {
        networkDBAdaptor.close();
    }

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

    public QueryResult<NetworkPath> pathQuery(NetworkPathQuery networkPathQuery, QueryOptions queryOptions)
            throws BioNetDBException {
        NetworkPathIterator networkPathIterator = pathIterator(networkPathQuery, queryOptions);
        return getQueryResult(networkPathIterator);
    }

    public QueryResult<NetworkPath> pathQuery(String cypher) throws BioNetDBException {
        NetworkPathIterator networkPathIterator = pathIterator(cypher);
        return getQueryResult(networkPathIterator);
    }

    public NetworkPathIterator pathIterator(NetworkPathQuery networkPathQuery, QueryOptions queryOptions) throws BioNetDBException {
        return networkDBAdaptor.networkPathIterator(networkPathQuery, queryOptions);
    }

    public NetworkPathIterator pathIterator(String cypher) throws BioNetDBException {
        return networkDBAdaptor.networkPathIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // N E T W O R K S
    //-------------------------------------------------------------------------

    public QueryResult<Network> networkQuery(List<NodeQuery> nodeQueries, QueryOptions queryOptions)
            throws BioNetDBException {
        return networkDBAdaptor.networkQuery(nodeQueries, queryOptions);
    }

    public QueryResult<Network> networkQueryByPaths(List<NetworkPathQuery> pathQueries, QueryOptions queryOptions)
            throws BioNetDBException {
        return networkDBAdaptor.networkQueryByPaths(pathQueries, queryOptions);
    }

    public QueryResult<Network> networkQuery(String cypher) throws BioNetDBException {
        return networkDBAdaptor.networkQuery(cypher);
    }


    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private List<Variant> convert(List<VariantContext> variantContexts, VariantContextToVariantConverter converter) {
        // Iterate over variant context and convert to variant
        List<Variant> variants = new ArrayList<>(variantContexts.size());
        for (VariantContext variantContext : variantContexts) {
            Variant variant = converter.convert(variantContext);
            variants.add(variant);
        }
        return variants;
    }


    private void updateNodeUids(Node.Type type, Query query, QueryOptions queryOptions, NetworkManager netManager)
            throws BioNetDBException {
        // Get network nodes
        List<Node> nodes = netManager.getNodes(type);

        for (Node node : nodes) {
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

    public QueryResult<NetworkPath> getQueryResult(NetworkPathIterator networkPathIterator)
            throws BioNetDBException {
        List<NetworkPath> networkPaths = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (networkPathIterator.hasNext()) {
            if (networkPaths.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            networkPaths.add(networkPathIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("get", time, networkPaths.size(), networkPaths.size(), null, null, networkPaths);
    }
}
