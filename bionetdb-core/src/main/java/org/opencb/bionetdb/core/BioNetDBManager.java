package org.opencb.bionetdb.core;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.NodeIterator;
import org.opencb.bionetdb.core.api.RowIterator;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.io.VariantParser;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JQueryParser;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.NetworkManager;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
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

        idToUidMap = new HashMap<>();

        init();
    }

    private void init() throws BioNetDBException {
        logger = LoggerFactory.getLogger(BioNetDBManager.class);

        networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration, true);
    }

    public void loadBiopax(Path path) throws IOException, BioNetDBException {
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

    public void loadVcf(Path path) throws BioNetDBException {
        // Be sure to initialize the network DB adapter
        init();

        // Variant parser
        VariantParser variantParser = new VariantParser();

        // VCF File reader management
        VcfFileReader vcfFileReader = new VcfFileReader(path.toString(), false);
        vcfFileReader.open();
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        variantParser.setSampleNames(vcfHeader.getSampleNamesInOrder());

        // VariantContext-to-Variant converter
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter("dataset",
                path.toFile().getName(), vcfFileReader.getVcfHeader().getSampleNamesInOrder());

        List<VariantContext> variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        while (variantContexts.size() == VARIANT_BATCH_SIZE) {
            // Convert to variants, parse and merge it into the final network
            processVariantContexts(variantContexts, converter, variantParser);

            // Read next batch
            variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);

            // TODO: only for test, process two batches, remove later
            break;
        }
        // Process the remaining variants
        if (variantContexts.size() > 0) {
            // Convert to variants, parse and merge it into the final network
            processVariantContexts(variantContexts, converter, variantParser);
        }

        // close VCF file reader
        vcfFileReader.close();
    }


    public void annotate() {

    }

    public void annotateGenes(Query query, QueryOptions queryOptions) {

    }

    public void annotateVariants(Query query, QueryOptions queryOptions) {

    }

    //-------------------------------------------------------------------------
    // N O D E S
    //-------------------------------------------------------------------------

    public QueryResult<Node> getNode(long uid) throws BioNetDBException {
        return nodeQuery("match (n) where n.uid = " + uid + " return n");
        //new Query(NetworkDBAdaptor.NetworkQueryParams.NODE_UID.key(), uid), QueryOptions.empty());
    }

    public QueryResult<Node> getNode(String id) throws BioNetDBException {
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), ":id=" + id);
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "node");
        return nodeQuery(query, QueryOptions.empty());
    }

    public QueryResult<Node> nodeQuery(Query query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parse(query, queryOptions);
        return nodeQuery(cypher);
    }

//    public List<QueryResult<Node>> getNode(List<String> id) throws BioNetDBException {
//        return null;
//    }

    public QueryResult<Node> nodeQuery(String cypher) throws BioNetDBException {
        List<Node> nodes = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        NodeIterator nodoIterator = nodeIterator(cypher);
        while (nodoIterator.hasNext()) {
            if (nodes.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            nodes.add(nodoIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("get", time, nodes.size(), nodes.size(), null, null, nodes);
    }

    public NodeIterator nodeIterator(Query query, QueryOptions queryOptions) {
        return networkDBAdaptor.nodeIterator(query, queryOptions);
    }

    public NodeIterator nodeIterator(String cypher) {
        return networkDBAdaptor.nodeIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // T A B L E S
    //   - a table is a list of rows
    //   - a row is a list of strings
    //-------------------------------------------------------------------------

    public QueryResult<List<Object>> table(Query query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parse(query, queryOptions);
        return table(cypher);
    }

    public QueryResult<List<Object>> table(String cypher) throws BioNetDBException {
        List<List<Object>> rows = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        RowIterator rowIterator = rowIterator(cypher);
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

    public RowIterator rowIterator(Query query, QueryOptions queryOptions) {
        return networkDBAdaptor.rowIterator(query, queryOptions);
    }

    public RowIterator rowIterator(String cypher) {
        return networkDBAdaptor.rowIterator(cypher);
    }


    //-------------------------------------------------------------------------
    // N E T W O R K S
    //-------------------------------------------------------------------------

    public QueryResult<Network> networkQuery(Query query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parse(query, queryOptions);
        return networkQuery(cypher);
    }

    public QueryResult<Network> networkQuery(String cypher) throws BioNetDBException {
        return networkDBAdaptor.networkQuery(cypher);
    }

    //-------------------------------------------------------------------------
    // A N A L Y S I S
    //-------------------------------------------------------------------------

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

    private void processVariantContexts(List<VariantContext> variantContexts, VariantContextToVariantConverter converter,
                                        VariantParser variantParser) throws BioNetDBException {
        // Convert to variants, parse and merge it into the final network
        List<Variant> variants = convert(variantContexts, converter);
        Network network = variantParser.parse(variants);
        NetworkManager netManager = new NetworkManager(network);

        Query query = new Query();

        // Update uid for variant nodes
        query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), Node.Type.VARIANT.name());
        QueryOptions queryOptions = QueryOptions.empty();
        updateNodeUids(Node.Type.VARIANT, query, queryOptions, netManager);

        // Update uid for sample nodes
        query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), Node.Type.SAMPLE.name());
        updateNodeUids(Node.Type.VARIANT, query, queryOptions, netManager);

//
//        List<Node> sampleNodes = netManager.getNodes(Node.Type.SAMPLE);
//        for (Node node: sampleNodes) {
//            System.out.println("node " + node.getType().name() + ": uid=" + node.getUid() + ", id=" + node.getId() + ", name="
//                    + node.getName());
//        }


        // Load network to the database
//        networkDBAdaptor.insert(network, QueryOptions.empty());
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
                query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_ID.key(), node.getId());
                QueryResult<Node> vNodes = networkDBAdaptor.queryNodes(query, queryOptions);
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
}
