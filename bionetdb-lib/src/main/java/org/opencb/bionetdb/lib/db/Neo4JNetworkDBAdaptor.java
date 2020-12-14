package org.opencb.bionetdb.lib.db;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.driver.*;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.*;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.iterators.NetworkPathIterator;
import org.opencb.bionetdb.lib.api.iterators.NodeIterator;
import org.opencb.bionetdb.lib.db.iterators.Neo4JNetworkPathIterator;
import org.opencb.bionetdb.lib.db.iterators.Neo4JNodeIterator;
import org.opencb.bionetdb.lib.db.query.Neo4JQueryParser;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.bionetdb.lib.utils.Utils.PREFIX_ATTRIBUTES;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private Driver driver;
    private BioNetDBConfiguration configuration;

    public Neo4JNetworkDBAdaptor(BioNetDBConfiguration configuration) {
        this.configuration = configuration;

        // configuration class checks if database is null or empty
        DatabaseConfiguration databaseConfiguration = configuration.getDatabase();

        String databaseURI = databaseConfiguration.getHost() + ":" + databaseConfiguration.getPort();
        String user = databaseConfiguration.getUser();
        String password = databaseConfiguration.getPassword();

        driver = GraphDatabase.driver("bolt://" + databaseURI, AuthTokens.basic(user, password));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> driver.close()));

//        // Add configuration node
//        if (!existConfigNode()) {
//            createConfigNode();
//        }
    }

    @Override
    public void close() {
        driver.close();
    }

    @Override
    public void index() {
        Session session = this.driver.session();
        if (session != null) {
            session.writeTransaction(tx -> {
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.GENE + ") ON (n.id)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.GENE + ") ON (n.name)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.GENE + ") ON (n.source)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.TRANSCRIPT + ") ON (n.id)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.TRANSCRIPT + ") ON (n.name)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.TRANSCRIPT + ") ON (n.source)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.PROTEIN + ") ON (n.id)");
                tx.run("CREATE INDEX IF NOT EXISTS FOR (n:" + Node.Type.PROTEIN + ") ON (n.name)");
                return 1;
            });


//            try (Transaction tx = session.beginTransaction()) {
//                tx.run("CREATE INDEX ON :" + Node.Type.PHYSICAL_ENTITY + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(name)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.COMPLEX + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SMALL_MOLECULE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.DNA + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.RNA + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(name)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PATHWAY + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.CATALYSIS + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.REGULATION + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.REACTION + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(" + PREFIX_ATTRIBUTES + "dbName)");
//                tx.run("CREATE INDEX ON :" + Node.Type.UNDEFINED + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(name)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_OBJECT + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_CALL + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_FILE_INFO + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.CONSEQUENCE_TYPE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.POPULATION_FREQUENCY + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.TRAIT_ASSOCIATION + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SAMPLE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SAMPLE + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SO + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SO + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.GENE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.GENE + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.DRUG + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.DRUG + "(name)");
//                tx.run("CREATE INDEX ON :" + Node.Type.DISEASE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.DISEASE + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.TRANSCRIPT + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.TRANSCRIPT + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.TFBS + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_KEYWORD + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_KEYWORD + "(name)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_FEATURE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.FUNCTIONAL_SCORE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SUBSTITUTION_SCORE + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_VARIANT_ANNOTATION + "(uid)");
//                tx.run("CREATE INDEX ON :" + Node.Type.CONSERVATION + "(uid)");
//
//                tx.run("CREATE INDEX ON :" + Node.Type.CONFIG + "(uid)");

//                tx.success();
            session.close();
        }
    }

    //-------------------------------------------------------------------------
    // I N S E R T     N E T W O R K S
    //-------------------------------------------------------------------------

    /**
     * Insert an entire network into the Neo4J database.
     *
     * @param network      Object containing all the nodes and interactions
     * @param queryOptions Optional params
     */
    @Override
    public void insert(Network network, QueryOptions queryOptions) throws BioNetDBException {
        Session session = this.driver.session();

        // First, insert Neo4J nodes
        for (Node node : network.getNodes()) {
            session.writeTransaction(tx -> {
                addNode(node, tx);
                return 1;
            });
        }

        // Second, insert Neo4J relationships
        for (Relation relation : network.getRelations()) {
            session.writeTransaction(tx -> {
                addRelation(relation, tx);
                return 1;
            });
        }

        session.close();
    }

    //-------------------------------------------------------------------------
    // A N N O T A T I O N     M E T H O D s
    //-------------------------------------------------------------------------

//    public void annotateVariants(NodeQuery query, QueryOptions options, VariantClient variantClient) throws BioNetDBException,
//            IOException {
//        NodeIterator nodeIterator = nodeIterator(query, options);
//        List<String> variantIds = new ArrayList<>(1000);
//        while (nodeIterator.hasNext()) {
//            Node variantNode = nodeIterator.next();
//            variantIds.add(variantNode.getId());
//            if (variantIds.size() >= 1000) {
//                annotateVariants(variantIds, variantClient);
//                variantIds.clear();
//            }
//        }
//        if (CollectionUtils.isNotEmpty(variantIds)) {
//            annotateVariants(variantIds, variantClient);
//        }
//    }
//
//    public void annotateVariants(List<String> variantIds, VariantClient variantClient) throws BioNetDBException, IOException {
//        Neo4JVariantLoader variantLoader = new Neo4JVariantLoader(this);
//        CellBaseDataResponse<Variant> entryQueryResponse = variantClient.get(variantIds, QueryOptions.empty());
//        for (CellBaseDataResult<Variant> queryResult : entryQueryResponse.getResponses()) {
//            if (CollectionUtils.isNotEmpty(queryResult.getResults())) {
//                variantLoader.loadVariants(queryResult.getResults());
//            }
//        }
//    }

    //-------------------------------------------------------------------------

//    public void annotateGenes(NodeQuery query, QueryOptions options, GeneClient geneClient) throws BioNetDBException,
//            IOException {
//        NodeIterator nodeIterator = nodeIterator(query, options);
//        List<String> geneIds = new ArrayList<>(1000);
//        while (nodeIterator.hasNext()) {
//            Node geneNode = nodeIterator.next();
//            geneIds.add(geneNode.getId());
//            if (geneIds.size() >= 1000) {
//                annotateGenes(geneIds, geneClient);
//                geneIds.clear();
//            }
//        }
//        if (CollectionUtils.isNotEmpty(geneIds)) {
//            annotateGenes(geneIds, geneClient);
//        }
//    }

//    public void annotateGenes(List<String> geneIds, GeneClient geneClient) throws BioNetDBException, IOException {
//        Neo4JVariantLoader variantLoader = new Neo4JVariantLoader(this);
//        QueryOptions options = new QueryOptions("EXCLUDE", "transcripts.exons,transcripts.cDnaSequence");
//        CellBaseDataResponse<Gene> entryQueryResponse = geneClient.get(geneIds, options);
//        for (CellBaseDataResult<Gene> queryResult : entryQueryResponse.getResponses()) {
//            if (CollectionUtils.isNotEmpty(queryResult.getResults())) {
//                variantLoader.loadGenes(queryResult.getResults());
//            }
//        }
//    }

    //-------------------------------------------------------------------------

//    public void annotateProteins(NodeQuery query, QueryOptions options, ProteinClient proteinClient) throws BioNetDBException,
//            IOException {
//        NodeIterator nodeIterator = nodeIterator(query, options);
//        List<String> proteinIds = new ArrayList<>(1000);
//        while (nodeIterator.hasNext()) {
//            Node geneNode = nodeIterator.next();
//            proteinIds.add(geneNode.getId());
//            if (proteinIds.size() >= 1000) {
//                annotateProteins(proteinIds, proteinClient);
//                proteinIds.clear();
//            }
//        }
//        if (CollectionUtils.isNotEmpty(proteinIds)) {
//            annotateProteins(proteinIds, proteinClient);
//        }
//    }

//    public void annotateProteins(List<String> proteinIds, ProteinClient proteinClient) throws BioNetDBException, IOException {
//        Neo4JVariantLoader variantLoader = new Neo4JVariantLoader(this);
//        QueryOptions options = new QueryOptions("EXCLUDE", "transcripts.exons,transcripts.cDnaSequence");
//        CellBaseDataResponse<Entry> entryQueryResponse = proteinClient.get(proteinIds, options);
//        for (CellBaseDataResult<Entry> responses : entryQueryResponse.getResponses()) {
//            if (CollectionUtils.isNotEmpty(responses.getResults())) {
//                variantLoader.loadProteins(responses.getResults());
//            }
//        }
//    }

//    public void annotateProtein(ProteinClient proteinClient) throws BioNetDBException, IOException {
//        // First, get all proteins from the network
//        Query query = new Query();
//        String cypher = "MATCH path=(p:PROTEIN)-[xr:XREF]->(x:XREF) WHERE toLower(x.attr_source) = \"uniprot\" return path";
//        QueryResult<Network> networkResult = networkQuery(cypher);
//
//        if (ListUtils.isEmpty(networkResult.getResult())) {
//            System.out.println("Network not found!!");
//            return;
//        }
//        Network network = networkResult.getResult().get(0);
//
//        // Get proteins annotations from Cellbase...
//        // ... prepare list of protein id/names from xref/protein nodes
//        List<String> proteinIds = new ArrayList<>();
//        for (Node node: network.getNodes()) {
//            if (node.getType() == Node.Type.XREF) {
//                proteinIds.add(node.getId());
//            }
//        }
//
//        // ... finally, call Cellbase service
//        Map<String, Entry> proteinMap = new HashMap<>();
//        QueryResponse<Entry> entryQueryResponse = proteinClient.get(proteinIds, new QueryOptions(QueryOptions.EXCLUDE,
//                "reference,organism,comment,evidence,sequence"));
//        for (QueryResult<Entry> queryResult: entryQueryResponse.getResponse()) {
//            proteinMap.put(queryResult.getId(), queryResult.getResult().get(0));
//        }
//
//        for (String key: proteinMap.keySet()) {
//            System.out.println(key + " -> " + proteinMap.get(key));
//        }
//    }

    //-------------------------------------------------------------------------
    // N O D E     Q U E R I E S
    //-------------------------------------------------------------------------

    @Override
    public NodeIterator nodeIterator(Query query, QueryOptions queryOptions) {
        String cypher = Neo4JQueryParser.parseNodeQuery(query, queryOptions);
        return nodeIterator(cypher);
    }

    @Override
    public NodeIterator nodeIterator(String cypher) {
        Session session = this.driver.session();
        System.out.println("Cypher query: " + cypher);
        return new Neo4JNodeIterator(session.run(cypher));
    }

    @Override
    public BioNetDBResult<Node> nodeQuery(Query query, QueryOptions queryOptions) {
        String cypher = Neo4JQueryParser.parseNodeQuery(query, queryOptions);
        return nodeQuery(cypher);
    }

    @Override
    public BioNetDBResult<Node> nodeQuery(String cypher) {
        // Query for nodes using the node iterator
        StopWatch stopWatch = StopWatch.createStarted();
        NodeIterator nodeIterator = nodeIterator(cypher);
        List<Node> nodes = new ArrayList<>();
        while (nodeIterator.hasNext()) {
            nodes.add(nodeIterator.next());
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        // Create QueryResult and return
        BioNetDBResult<Node> queryResult = new BioNetDBResult<>(dbTime, new ArrayList<>(), nodes.size(), nodes, nodes.size());
        return queryResult;
    }

    @Override
    public BioNetDBResult<NodeStats> nodeStats(Query query) {
        StopWatch stopWatch = StopWatch.createStarted();
        Session session = this.driver.session();

        StringBuilder where = new StringBuilder();
        List<String> filters = Neo4JQueryParser.getFilters("n", query);
        if (filters.size() > 0) {
            where.append(" where ").append(StringUtils.join(filters, " and"));
        }
        String cypher = "match (n)" + where.toString() + " with distinct labels(n) as label, count(labels(n)) as cnt return label, cnt";
        System.out.println("Cypher query: " + cypher);
        Result result = session.run(cypher);

        long total = 0;
        Map<String, Long> count = new HashMap<>();

        while (result.hasNext()) {
            Record record = result.next();
            List<Object> items = record.get(0).asList();
            long cnt = record.get(1).asLong();
            for (Object item: items) {
                String label = (String) item;
                if (!count.containsKey(label)) {
                    count.put(label, 0L);
                }
                count.put(label, count.get(label) + cnt);
            }
            total += cnt;
        }

        NodeStats stats = new NodeStats(total, count);
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        return new BioNetDBResult<>(dbTime, Collections.emptyList(), 1, Collections.singletonList(stats), 1);
    }

    //-------------------------------------------------------------------------
    // T A B L E     Q U E R I E S
    //-------------------------------------------------------------------------

//    @Override
//    public RowIterator rowIterator(Query query, QueryOptions queryOptions) throws BioNetDBException {
//        String cypher = Neo4JQueryParser.parseNodeQuery(query, queryOptions);
//        return rowIterator(cypher);
//    }
//
//    public RowIterator rowIterator(String cypher) throws BioNetDBException {
//        Session session = this.driver.session();
//        System.out.println("Cypher query: " + cypher);
//        return new Neo4JRowIterator(session.run(cypher));
//    }
//
//    @Override
//    public DataResult<List<Object>> rowQuery(Query query, QueryOptions queryOptions) throws BioNetDBException {
//        throw new UnsupportedOperationException("rowQuery not yet supported");
////        String cypher = Neo4JQueryParser.parseRowQuery(query, queryOptions);
////        return nodeQuery(cypher);
//    }
//
//    @Override
//    public DataResult<List<Object>> rowQuery(String cypher) throws BioNetDBException {
//        throw new UnsupportedOperationException("rowQuery not yet supported");
//    }

    //-------------------------------------------------------------------------
    // P A T H     Q U E R I E S
    //-------------------------------------------------------------------------

    @Override
    public NetworkPathIterator networkPathIterator(Query networkPathQuery, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parseNetworkPathQuery(networkPathQuery, queryOptions);
        return networkPathIterator(cypher);
    }

    @Override
    public NetworkPathIterator networkPathIterator(String cypher) {
        Session session = this.driver.session();
        System.out.println("Cypher query: " + cypher);
        return new Neo4JNetworkPathIterator(session.run(cypher));
    }

    @Override
    public BioNetDBResult<NetworkPath> networkPathQuery(Query query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parseNetworkPathQuery(query, queryOptions);
        return networkPathQuery(cypher);
    }

    @Override
    public BioNetDBResult<NetworkPath> networkPathQuery(String cypher) {
        // Query for nodes using the node iterator
        StopWatch stopWatch = StopWatch.createStarted();
        NetworkPathIterator pathIterator = networkPathIterator(cypher);
        List<NetworkPath> networkPaths = new ArrayList<>();
        while (pathIterator.hasNext()) {
            networkPaths.add(pathIterator.next());
        }
        int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

        // Create QueryResult and return
        BioNetDBResult<NetworkPath> queryResult = new BioNetDBResult<>(dbTime, new ArrayList<>(), networkPaths.size(), networkPaths,
                networkPaths.size());
        return queryResult;
    }

    //-------------------------------------------------------------------------
    // N E T W O R K     Q U E R I E S
    //-------------------------------------------------------------------------

//    @Override
//    public DataResult<Network> networkQuery(List<NodeQuery> nodeQueries, QueryOptions queryOptions)
//            throws BioNetDBException {
//        String cypher = Neo4JQueryParser.parseNodesForNetwork(nodeQueries, queryOptions);
//        return networkQuery(cypher);
//    }
//
//    @Override
//    public DataResult<Network> networkQueryByPaths(List<NetworkPathQuery> pathQueries, QueryOptions queryOptions)
//            throws BioNetDBException {
//        String cypher = Neo4JQueryParser.parsePathsForNetwork(pathQueries, queryOptions);
//        return networkQuery(cypher);
//    }
//
//    @Override
//    public DataResult<Network> networkQuery(String cypher) throws BioNetDBException {
//        // Open session
//        Session session = this.driver.session();
//
//        long startTime = System.currentTimeMillis();
//        Result run = session.run(cypher);
//        Network network = Neo4jConverter.toNetwork(run);
//        long stopTime = System.currentTimeMillis();
//
//        // Close session
//        session.close();
//
//        // Build the query result
//        int dbTime = (int) (stopTime - startTime) / 1000;
//        return new DataResult(dbTime, new ArrayList<>(), 1, Collections.singletonList(network), 1);
//    }
//
//    @Override
//    public VariantIterator variantIterator(Query query, QueryOptions queryOptions) throws BioNetDBException {
//        return null;
//    }

    //-------------------------------------------------------------------------
    // V A R I A N T
    //-------------------------------------------------------------------------

//    public VariantIterator variantIterator(String cypher) throws BioNetDBException {
//        Session session = this.driver.session();
//        System.out.println("Cypher query: " + cypher);
//        return new Neo4JVariantIterator(session.run(cypher));
//    }
//
//    @Override
//    public DataResult<Variant> variantQuery(Query query, QueryOptions queryOptions) throws BioNetDBException {
//        String cypher = Neo4JVariantQueryParser.parse(query, QueryOptions.empty());
//        return variantQuery(cypher);
//    }
//
//    @Override
//    public DataResult<Variant> variantQuery(String cypher) throws BioNetDBException {
//        if (org.apache.commons.lang.StringUtils.isEmpty(cypher)) {
//            throw new BioNetDBException("Missing cypher to query");
//        }
//
//        List<Variant> variants = new ArrayList<>();
//        VariantIterator variantIterator = variantIterator(cypher);
//        while (variantIterator.hasNext()) {
//            Variant variant = variantIterator.next();
//            if (variant != null) {
//                variants.add(variant);
//            }
//        }
//
//        int dbTime = 0;
//        return new DataResult(dbTime, new ArrayList<>(), variants.size(), variants, variants.size());
//    }

    //-------------------------------------------------------------------------
    // I N T E R P R  E T A T I O N     A N A L Y S I S
    //-------------------------------------------------------------------------

//    @Override
//    public DataResult<Variant> proteinNetworkInterpretationAnalysis(boolean complexOrReaction, Query query) throws BioNetDBException {
//        // Create cypher statement from query
//        String cypher = Neo4JVariantQueryParser.parseProteinNetworkInterpretation(query, QueryOptions.empty(), complexOrReaction);
//
//        // The next code is performed also by queryVariants method in MoIManager
//        List<Variant> variants = new ArrayList<>();
//
//        VariantIterator variantIterator = variantIterator(cypher);
//
//        while (variantIterator.hasNext()) {
//            variants.add(variantIterator.next());
//        }
//
//        int dbTime = 0;
//        return new DataResult(dbTime, new ArrayList<>(), variants.size(), variants, variants.size());
//    }

    //-------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------------------

    public Result addNode(Node node, Transaction tx) {
        // Gather properties of the node to create a cypher string with them
        List<String> props = new ArrayList<>();
        props.add("n.uid=" + node.getUid());
        if (StringUtils.isNotEmpty(node.getId())) {
            props.add("n.id=\"" + cleanValue(node.getId()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getName())) {
            props.add("n.name=\"" + cleanValue(node.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getSource())) {
            props.add("n.source=\"" + node.getSource() + "\"");
        }
        if (node.getAttributes().containsKey("uidCounter")) {
            props.add("n." + PREFIX_ATTRIBUTES + "uidCounter=" + node.getAttributes().get("uidCounter"));
        }
        for (String key : node.getAttributes().keySet()) {
            if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                props.add("n." + PREFIX_ATTRIBUTES + key + "=" + node.getAttributes().getString(key));
            } else {
                props.add("n." + PREFIX_ATTRIBUTES + key + "=\"" + cleanValue(node.getAttributes().getString(key)) + "\"");
            }
        }
        //String propsJoined = "{" + String.join(",", props) + "}";

        // Create the desired node
        StringBuilder cypher = new StringBuilder("CREATE (n");
        if (CollectionUtils.isNotEmpty(node.getTags())) {
            cypher.append(":").append(StringUtils.join(node.getTags(), ":"));
        }
        cypher.append(")");
        if (CollectionUtils.isNotEmpty(props)) {
            cypher.append(" SET ").append(StringUtils.join(props, ","));
        }
        //cypher.append(" RETURN ID(n) AS UID");
        Result ret = tx.run(cypher.toString());
        //node.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    public void mergeNode(Node node, String byKey, Transaction tx) {
        Object value;
        switch (byKey) {
            case "id":
                value = node.getId();
                break;
            case "name":
                value = node.getName();
                break;
            case "source":
                value = node.getSource();
                break;
            default:
                value = node.getAttributes().get(byKey);
                break;
        }
        StringBuilder cypher = new StringBuilder("MATCH (n");
        if (node.getType() != null) {
            cypher.append(":").append(node.getType());
        }
        cypher.append(") WHERE n.").append(byKey);
        if (value instanceof String) {
            cypher.append("=\"").append(cleanValue(value.toString())).append("\"");
        } else {
            cypher.append("=").append(value);
        }
        cypher.append(" RETURN n");

        Result ret = tx.run(cypher.toString());
        if (ret.hasNext()) {
            node.setUid(ret.next().get(0).asNode().get("uid").asLong());
        } else {
            node.setUid(node.getUid() + 1);
            addNode(node, tx);
        }
    }

    public void mergeNode(Node node, String byKey1, String byKey2, Transaction tx) {
        List<String> byKeys = new ArrayList<>(2);
        byKeys.add(byKey1);
        byKeys.add(byKey2);
        List<Object> values = new ArrayList<>(2);
        List<Boolean> prefix = new ArrayList<>(2);

        for (int i = 0; i < 2; i++) {
            switch (byKeys.get(i)) {
                case "id":
                    values.add(node.getId());
                    prefix.add(false);
                    break;
                case "name":
                    values.add(node.getName());
                    prefix.add(false);
                    break;
                case "source":
                    values.add(node.getSource());
                    prefix.add(false);
                    break;
                default:
                    values.add(node.getAttributes().get(byKeys.get(i)));
                    prefix.add(true);
                    break;
            }
        }
        StringBuilder cypher = new StringBuilder("MATCH (n");
        if (node.getType() != null) {
            cypher.append(":").append(node.getType());
        }
        cypher.append(") WHERE ");
        for (int i = 0; i < 2; i++) {
            cypher.append("n.");
            if (prefix.get(i)) {
                cypher.append(PREFIX_ATTRIBUTES + byKeys.get(i));
            } else {
                cypher.append(byKeys.get(i));
            }
            if (values.get(i) instanceof String) {
                cypher.append("=\"").append(cleanValue(values.get(i).toString())).append("\"");
            } else {
                cypher.append("=").append(values.get(i));
            }
            if (i == 0) {
                cypher.append(" AND ");
            }
        }
        cypher.append(" RETURN n");

        Result ret = tx.run(cypher.toString());
        if (ret.hasNext()) {
            node.setUid(ret.next().get(0).asNode().get("uid").asLong());
        } else {
            node.setUid(node.getUid() + 1);
            addNode(node, tx);
        }
    }

    public Result mergeRelation(Relation relation, Transaction tx) {
        return addRelation(relation, tx);
    }

    public Result addRelation(Relation relation, Transaction tx) {
        List<String> props = new ArrayList<>();
        //props.add("uid:" + relation.getUid());
        if (StringUtils.isNotEmpty(relation.getName())) {
            props.add("name:\"" + cleanValue(relation.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(relation.getSource())) {
            props.add("source:\"" + relation.getSource() + "\"");
        }
        for (String key : relation.getAttributes().keySet()) {
            props.add(PREFIX_ATTRIBUTES + key + ":\"" + cleanValue(relation.getAttributes().getString(key)) + "\"");
        }
        String propsJoined = "{" + String.join(",", props) + "}";

        StringBuilder statementTemplate = new StringBuilder();
        statementTemplate.append("MATCH (o:").append(relation.getOrigType()).append("{uid:")
                .append(relation.getOrigUid()).append("}) MATCH (d:").append(relation.getDestType()).append("{uid:")
                .append(relation.getDestUid()).append("}) USING INDEX d:").append(relation.getDestType())
                .append("(uid) MERGE (o)-[r:")
                .append(StringUtils.join(relation.getTags(), ":"));
        if (CollectionUtils.isNotEmpty(props)) {
            statementTemplate.append(propsJoined);
        }
        statementTemplate.append("]->(d)");

        //.append(" RETURN ID(r) AS UID");

        // Create the relationship
        Result ret = tx.run(statementTemplate.toString());
        //relation.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    //-------------------------------------------------------------------------
    // C O N F I G U R A T I O N     N O D E     M A N A G E M E N T
    //-------------------------------------------------------------------------

    private boolean existConfigNode() {
        Session session = this.driver.session();
        Result statementResult = session.run("match (n:" + Node.Type.CONFIG + "{uid:0}) return count(n) as count");
        session.close();

        return (statementResult.peek().get(0).asInt() == 1);
    }

    public long getUidCounter() {
        Session session = this.driver.session();
        Result statementResult = session.run("match (n{uid:0}) return n." + PREFIX_ATTRIBUTES + "uidCounter");
        session.close();

        return (statementResult.peek().get(0).asLong());
    }

    public void setUidCounter(long uidCounter) {
        // Build Cypher statement
        StringBuilder cypher = new StringBuilder();
        cypher.append("merge (n{uid:0}) set n.").append(PREFIX_ATTRIBUTES).append("uidCounter=").append(uidCounter);

        // Run cypher statement
        Session session = this.driver.session();
        session.run(cypher.toString());
        session.close();
    }

    private void createConfigNode() {
        // Create configuration node and set it uid = 0
        Session session = this.driver.session();
        Node node = new Node(0, "0", "config", Node.Type.CONFIG);
        node.addAttribute("uidCounter", 1);
        try (Transaction tx = session.beginTransaction()) {
            addNode(node, tx);
            //tx.success();
        }
        session.close();
    }

    private void updateConfigNode(ObjectMap attrs) {
        // Build Cypher statement
        StringBuilder cypher = new StringBuilder();
        cypher.append("merge (n{uid:0}) set ");
        List<String> sets = new ArrayList<>();
        for (String key : attrs.keySet()) {
            StringBuilder strSet = new StringBuilder("n.").append(PREFIX_ATTRIBUTES).append(key).append("=");
            if (attrs.get(key) instanceof String) {
                strSet.append("'").append(attrs.get(key)).append("'");
            } else {
                strSet.append(attrs.get(key));
            }
            sets.add(strSet.toString());
        }
        cypher.append(StringUtils.join(sets, ","));

        // Run cypher statement
        Session session = this.driver.session();
        try (Transaction tx = session.beginTransaction()) {
            tx.run(cypher.toString());
            //tx.success();
        }
        session.close();
    }

    //-------------------------------------------------------------------------
    // T E S T S
    //-------------------------------------------------------------------------

    public void loadTest() {
        Session session = this.driver.session();

        Long startUid = System.currentTimeMillis();
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Node node = new Node(i, "id_" + i, "name_" + i, Node.Type.CELLULAR_LOCATION, "test");
            node.getAttributes().put("uid", node.getUid());
            node.getAttributes().put("id", node.getId());
            node.getAttributes().put("name", node.getName());
            node.getAttributes().put("source", node.getSource());
            nodes.add(node);
        }

        long uid = startUid;
        System.out.println("Inserting nodes:");
        for (int j = 0; j < 10; j++) {
            long start = System.currentTimeMillis();
            try (Transaction tx = session.beginTransaction()) {
//                String query = "CREATE (n:PROTEIN{id:{id}, name:{name}}) RETURN ID(n) as ID";
                String cypher = "CREATE (n:CELLULAR_LOCATION) SET n = {map}";
                Map<String, Object> params = new HashMap<>();
                for (int i = 0; i < 1000; i++) {
                    Node node = nodes.get(1000 * j + i);
                    node.getAttributes().put("uid", uid++);
                    params.put("map", node.getAttributes());
                    Result ret = tx.run(cypher, params);
//                    node.setUid(ret.peek().get("ID").asLong());

//                    String cypher = "CREATE (n:PROTEIN)";
                    //String cypher = "CREATE (n:" + node.getType() + ") SET n.id = '" + node.getId() + "'";
                    // SET n.uid = " + (node.getUid() + (nodes.size() * j)) + ", n.name = '" + node.getName() + "', n.id = '"
                    // + node.getId() + "'";
//                    String cypher = "CREATE (n:" + node.getType() + ") SET n.uid = " + (node.getUid() + (nodes.size() * j))
// + ", n.name = '" + node.getName() + "', n.id = '" + node.getId() + "'";
//                    String cypher = "CREATE (n:PROTEIN{id:'" + node.getId() + "', name:'" + node.getName() + "'}) RETURN ID(n)";
//                    StatementResult ret = tx.run(cypher);

//                    StatementResult ret = tx.run(query, Values.parameters("id", node.getId(), "name", node.getName()));
//                    node.setUid(ret.peek().get("ID").asLong());

//            session.writeTransaction(tx -> {
//                String cypher = "CREATE (n:PROTEIN)"; // RETURN ID(n) as ID";
//                StatementResult ret = tx.run(cypher);
                    // long uid = ret.peek().get("ID").asLong();
//                //addNode(node, tx);
//                return 1;
//            });
                }
                //tx.success();
            }
//            System.out.println("Inserting " + nodes.size() + " nodes at " + (1000 * nodes.size() / (System.currentTimeMillis() - start))
// + " nodes/sec");
            System.out.println(1000 * 1000 / (System.currentTimeMillis() - start));
        }

        System.out.println("Update nodes:");
        Random r = new Random();
        for (int j = 0; j < 10; j++) {
            long start = System.currentTimeMillis();
            StringBuilder cypher = new StringBuilder();
            Map<String, Object> params = new HashMap<>();
            try (Transaction tx = session.beginTransaction()) {
                for (int i = 0; i < 1000; i++) {
                    Node node = nodes.get(r.nextInt(10000));

                    cypher.setLength(0);
                    cypher.append("merge (n:").append(Node.Type.CELLULAR_LOCATION);
                    cypher.append("{name:'").append(node.getName());
                    cypher.append("'})");
                    cypher.append(" set n.toto={toto}");
                    cypher.append(" return n.uid as uid");

                    Result ret = tx.run(cypher.toString(), Values.parameters("toto", "hello"));
                    //long retUid = ret.peek().get("uid").asLong();
                }
                //tx.success();
            }
            System.out.println(1000 * 1000 / (System.currentTimeMillis() - start));
        }

        System.out.println("Inserting relationships:");
        for (int j = 0; j < 10; j++) {
            long start = System.currentTimeMillis();
            StringBuilder cypher = new StringBuilder();
            try (Transaction tx = session.beginTransaction()) {
                for (long i = startUid; i < (startUid + 1000); i++) {
                    cypher.setLength(0);
                    long sourceUid = i + (j * 1000);
                    cypher.append("match (s:").append(Node.Type.CELLULAR_LOCATION);
                    cypher.append("), (d:").append(Node.Type.CELLULAR_LOCATION);
                    cypher.append(") where s.uid=").append(sourceUid).append(" AND d.uid=").append(sourceUid + 1000);

//                    cypher.append(" match (d:").append(Node.Type.CELLULAR_LOCATION).append(") where d.uid=").append(sourceUid + 1000);
//                    cypher.append("match (s:").append(Node.Type.CELLULAR_LOCATION).append(") where s.uid=").append(sourceUid);
//                    cypher.append(" match (d:").append(Node.Type.CELLULAR_LOCATION).append(") where d.uid=").append(sourceUid + 1000);
                    cypher.append(" create (s)-[r:").append(Node.Type.CELLULAR_LOCATION).append("{uid:").append(++uid).append("}]->(d)");
//                    cypher.append(" return r.uid");
                    Result ret = tx.run(cypher.toString());

//                String query = "CREATE (n:PROTEIN{id:{id}, name:{name}}) RETURN ID(n) as ID";
//                String cypher = "CREATE (n:CELLULAR_LOCATION) SET n = {map}";
//                Map<String, Object> params = new HashMap<>();
//                for (Node node: nodes) {
//                    node.getAttributes().put("uid", uid++);
//                    params.put("map", node.getAttributes());
//                    StatementResult ret = tx.run(cypher, params);
////                    node.setUid(ret.peek().get("ID").asLong());
//
////                    String cypher = "CREATE (n:PROTEIN)";
//                    //String cypher = "CREATE (n:" + node.getType() + ") SET n.id = '" + node.getId() + "'"; //SET n.uid = "
// + (node.getUid() + (nodes.size() * j)) + ", n.name = '" + node.getName() + "', n.id = '" + node.getId() + "'";
////                    String cypher = "CREATE (n:" + node.getType() + ") SET n.uid = " + (node.getUid() + (nodes.size() * j))
// + ", n.name = '" + node.getName() + "', n.id = '" + node.getId() + "'";
////                    String cypher = "CREATE (n:PROTEIN{id:'" + node.getId() + "', name:'" + node.getName() + "'}) RETURN ID(n)";
////                    StatementResult ret = tx.run(cypher);
//
////                    StatementResult ret = tx.run(query, Values.parameters("id", node.getId(), "name", node.getName()));
////                    node.setUid(ret.peek().get("ID").asLong());
//
////            session.writeTransaction(tx -> {
////                String cypher = "CREATE (n:PROTEIN)"; // RETURN ID(n) as ID";
////                StatementResult ret = tx.run(cypher);
//                    // long uid = ret.peek().get("ID").asLong();
////                //addNode(node, tx);
////                return 1;
////            });
                }
                //tx.success();
            }
//            System.out.println("Inserting " + nodes.size() + " nodes at " + (1000 * nodes.size() / (System.currentTimeMillis() - start))
// + " nodes/sec");
            System.out.println(1000 * 1000 / (System.currentTimeMillis() - start));
        }

        session.close();
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

//    private String fixString(String in) {
//        return "\"" + in + "\"";
//    }

    private String cleanValue(String value) {
        return value.replace("\"", ",").replace("\\", "|");
    }

    public Driver getDriver() {
        return driver;
    }

    public Neo4JNetworkDBAdaptor setDriver(Driver driver) {
        this.driver = driver;
        return this;
    }
}
