package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.NetworkPathIterator;
import org.opencb.bionetdb.core.api.NodeIterator;
import org.opencb.bionetdb.core.api.RowIterator;
import org.opencb.bionetdb.core.api.query.NetworkPathQuery;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.Neo4JConverter;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.cellbase.client.rest.VariationClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private Driver driver;
    private BioNetDBConfiguration configuration;

    public static final String PREFIX_ATTRIBUTES = "attr_";

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration) throws BioNetDBException {
        this(database, configuration, false);
    }

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration, boolean createIndex) throws BioNetDBException {
        this.configuration = configuration;

        // configuration class checks if database is null or empty
        DatabaseConfiguration databaseConfiguration = configuration.findDatabase(database);
        if (databaseConfiguration == null) {
            throw new BioNetDBException("No database found with name: \"" + database + "\"");
        }
        String databaseURI = databaseConfiguration.getHost() + ":" + databaseConfiguration.getPort();
        String user = databaseConfiguration.getUser();
        String password = databaseConfiguration.getPassword();

        driver = GraphDatabase.driver("bolt://" + databaseURI, AuthTokens.basic(user, password));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> driver.close()));

        // Create indexes
        if (createIndex) {
            createIndexes();
        }

        // Add configuration node
        if (!existConfigNode()) {
        createConfigNode();
        }
    }

    private void createIndexes() {
        Session session = this.driver.session();
        if (session != null) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :" + Node.Type.PHYSICAL_ENTITY + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(name)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.COMPLEX + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SMALL_MOLECULE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.DNA + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.RNA + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(name)");
                tx.run("CREATE INDEX ON :" + Node.Type.PATHWAY + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CATALYSIS + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.REGULATION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.REACTION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(" + PREFIX_ATTRIBUTES + "dbName)");
                tx.run("CREATE INDEX ON :" + Node.Type.UNDEFINED + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(name)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_CALL + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_FILE_INFO + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CONSEQUENCE_TYPE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.POPULATION_FREQUENCY + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.TRAIT_ASSOCIATION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SAMPLE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SAMPLE + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.SO + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SO + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.GENE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.GENE + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.DRUG + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.DRUG + "(name)");
                tx.run("CREATE INDEX ON :" + Node.Type.DISEASE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.DISEASE + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.TRANSCRIPT + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.TRANSCRIPT + "(id)");
                tx.run("CREATE INDEX ON :" + Node.Type.TFBS + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_KEYWORD + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_KEYWORD + "(name)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_FEATURE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.FUNCTIONAL_SCORE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SUBSTITUTION_SCORE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN_VARIANT_ANNOTATION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CONSERVATION + "(uid)");

                tx.run("CREATE INDEX ON :" + Node.Type.CONFIG + "(uid)");

                tx.success();
            }
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
        for (Node node: network.getNodes()) {
            session.writeTransaction(tx -> {
                addNode(node, tx);
                return 1;
            });
        }

        // Second, insert Neo4J relationships
        for (Relation relation: network.getRelations()) {
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

    public void annotateVariants(NodeQuery query, QueryOptions options, VariationClient variationClient) throws BioNetDBException,
            IOException {
        NodeIterator nodeIterator = nodeIterator(query, options);
        List<String> variantIds = new ArrayList<>(1000);
        while (nodeIterator.hasNext()) {
            Node variantNode = nodeIterator.next();
            variantIds.add(variantNode.getId());
            if (variantIds.size() >= 1000) {
                annotateVariants(variantIds, variationClient);
                variantIds.clear();
            }
        }
        if (ListUtils.isNotEmpty(variantIds)) {
            annotateVariants(variantIds, variationClient);
        }
    }

    public void annotateVariants(List<String> variantIds, VariationClient variationClient) throws BioNetDBException, IOException {
        Neo4JVariantLoader variantLoader = new Neo4JVariantLoader(this);
        QueryResponse<Variant> entryQueryResponse = variationClient.get(variantIds, QueryOptions.empty());
        for (QueryResult<Variant> queryResult: entryQueryResponse.getResponse()) {
            if (ListUtils.isNotEmpty(queryResult.getResult())) {
                variantLoader.loadVariants(queryResult.getResult());
            }
        }
    }

    //-------------------------------------------------------------------------

    public void annotateGenes(NodeQuery query, QueryOptions options, GeneClient geneClient) throws BioNetDBException,
            IOException {
        NodeIterator nodeIterator = nodeIterator(query, options);
        List<String> geneIds = new ArrayList<>(1000);
        while (nodeIterator.hasNext()) {
            Node geneNode = nodeIterator.next();
            geneIds.add(geneNode.getId());
            if (geneIds.size() >= 1000) {
                annotateGenes(geneIds, geneClient);
                geneIds.clear();
            }
        }
        if (ListUtils.isNotEmpty(geneIds)) {
            annotateGenes(geneIds, geneClient);
        }
    }

    public void annotateGenes(List<String> geneIds, GeneClient geneClient) throws BioNetDBException, IOException {
        Neo4JVariantLoader variantLoader = new Neo4JVariantLoader(this);
        QueryOptions options = new QueryOptions("EXCLUDE", "transcripts.exons,transcripts.cDnaSequence");
        QueryResponse<Gene> entryQueryResponse = geneClient.get(geneIds, options);
        for (QueryResult<Gene> queryResult: entryQueryResponse.getResponse()) {
            if (ListUtils.isNotEmpty(queryResult.getResult())) {
                variantLoader.loadGenes(queryResult.getResult());
            }
        }
    }

    //-------------------------------------------------------------------------

    public void annotateProteins(NodeQuery query, QueryOptions options, ProteinClient proteinClient) throws BioNetDBException,
            IOException {
        NodeIterator nodeIterator = nodeIterator(query, options);
        List<String> proteinIds = new ArrayList<>(1000);
        while (nodeIterator.hasNext()) {
            Node geneNode = nodeIterator.next();
            proteinIds.add(geneNode.getId());
            if (proteinIds.size() >= 1000) {
                annotateProteins(proteinIds, proteinClient);
                proteinIds.clear();
            }
        }
        if (ListUtils.isNotEmpty(proteinIds)) {
            annotateProteins(proteinIds, proteinClient);
        }
    }

    public void annotateProteins(List<String> proteinIds, ProteinClient proteinClient) throws BioNetDBException, IOException {
        Neo4JVariantLoader variantLoader = new Neo4JVariantLoader(this);
        QueryOptions options = new QueryOptions("EXCLUDE", "transcripts.exons,transcripts.cDnaSequence");
        QueryResponse<Entry> entryQueryResponse = proteinClient.get(proteinIds, options);
        for (QueryResult<Entry> queryResult: entryQueryResponse.getResponse()) {
            if (ListUtils.isNotEmpty(queryResult.getResult())) {
                variantLoader.loadProteins(queryResult.getResult());
            }
        }
    }

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
    public NodeIterator nodeIterator(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parseNode(query, queryOptions);
        return nodeIterator(cypher);
    }

    @Override
    public NodeIterator nodeIterator(String cypher) throws BioNetDBException {
        Session session = this.driver.session();
        return new Neo4JNodeIterator(session.run(cypher));
    }

    @Override
    public List<Node> nodeQuery(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parseNode(query, queryOptions);
        return nodeQuery(cypher);
    }

    @Override
    public List<Node> nodeQuery(String cypher) throws BioNetDBException {
        Session session = this.driver.session();
        NodeIterator nodeIterator = nodeIterator(cypher);
        List<Node> nodes = new ArrayList<>();
        while (nodeIterator.hasNext()) {
            nodes.add(nodeIterator.next());
        }
        return nodes;
    }
    //-------------------------------------------------------------------------
    // T A B L E     Q U E R I E S
    //-------------------------------------------------------------------------

    @Override
    public RowIterator rowIterator(NodeQuery query, QueryOptions queryOptions) throws BioNetDBException {
        String cypher = Neo4JQueryParser.parseNode(query, queryOptions);
        return rowIterator(cypher);
    }

    public RowIterator rowIterator(String cypher) throws BioNetDBException {
        Session session = this.driver.session();
//        System.out.println("Cypher query: " + cypher);
        return new Neo4JRowIterator(session.run(cypher));
    }

    //-------------------------------------------------------------------------
    // P A T H     Q U E R I E S
    //-------------------------------------------------------------------------

    @Override
    public NetworkPathIterator networkPathIterator(NetworkPathQuery networkPathQuery, QueryOptions queryOptions)
            throws BioNetDBException {

        String cypher = Neo4JQueryParser.parsePath(networkPathQuery, queryOptions);
        return networkPathIterator(cypher);
    }

    @Override
    public NetworkPathIterator networkPathIterator(String cypher) throws BioNetDBException {
        Session session = this.driver.session();
//        System.out.println("Cypher query: " + cypher);
        return new Neo4JNetworkPathIterator(session.run(cypher));
    }

    //-------------------------------------------------------------------------
    // N E T W O R K     Q U E R I E S
    //-------------------------------------------------------------------------

    @Override
    public QueryResult<Network> networkQuery(List<NodeQuery> nodeQueries, QueryOptions queryOptions)
            throws BioNetDBException {
        String cypher = Neo4JQueryParser.parseNodesForNetwork(nodeQueries, queryOptions);
        return networkQuery(cypher);
    }

    @Override
    public QueryResult<Network> networkQueryByPaths(List<NetworkPathQuery> pathQueries, QueryOptions queryOptions)
            throws BioNetDBException {
        String cypher = Neo4JQueryParser.parsePathsForNetwork(pathQueries, queryOptions);
        return networkQuery(cypher);
    }

    @Override
    public QueryResult<Network> networkQuery(String cypher) throws BioNetDBException {
        // Open session
        Session session = this.driver.session();

        long startTime = System.currentTimeMillis();
        StatementResult run = session.run(cypher);
        Network network = Neo4JConverter.toNetwork(run);
        long stopTime = System.currentTimeMillis();

        // Close session
        session.close();

        // Build the query result
        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("networkQuery", time, 1, 1, null, null, Arrays.asList(network));
    }

    public StatementResult addNode(Node node, Transaction tx) {
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
        for (String key: node.getAttributes().keySet()) {
            if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                props.add("n." + PREFIX_ATTRIBUTES + key + "=" + node.getAttributes().getString(key));
            } else {
                props.add("n." + PREFIX_ATTRIBUTES + key + "=\"" + cleanValue(node.getAttributes().getString(key)) + "\"");
            }
        }
        //String propsJoined = "{" + String.join(",", props) + "}";

        // Create the desired node
        StringBuilder cypher = new StringBuilder("CREATE (n");
        if (ListUtils.isNotEmpty(node.getTags())) {
            cypher.append(":").append(StringUtils.join(node.getTags(), ":"));
        }
        cypher.append(")");
        if (ListUtils.isNotEmpty(props)) {
            cypher.append(" SET ").append(StringUtils.join(props, ","));
        }
        //cypher.append(" RETURN ID(n) AS UID");
        StatementResult ret = tx.run(cypher.toString());
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

        StatementResult ret = tx.run(cypher.toString());
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

        StatementResult ret = tx.run(cypher.toString());
        if (ret.hasNext()) {
            node.setUid(ret.next().get(0).asNode().get("uid").asLong());
        } else {
            node.setUid(node.getUid() + 1);
            addNode(node, tx);
        }
    }

    public StatementResult mergeRelation(Relation relation, Transaction tx) {
        return addRelation(relation, tx);
    }

    public StatementResult addRelation(Relation relation, Transaction tx) {
        List<String> props = new ArrayList<>();
        //props.add("uid:" + relation.getUid());
        if (StringUtils.isNotEmpty(relation.getName())) {
            props.add("name:\"" + cleanValue(relation.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(relation.getSource())) {
            props.add("source:\"" + relation.getSource() + "\"");
        }
        for (String key: relation.getAttributes().keySet()) {
            props.add(PREFIX_ATTRIBUTES + key + ":\"" + cleanValue(relation.getAttributes().getString(key)) + "\"");
        }
        String propsJoined = "{" + String.join(",", props) + "}";

        StringBuilder statementTemplate = new StringBuilder();
        statementTemplate.append("MATCH (o:").append(relation.getOrigType()).append("{uid:")
                .append(relation.getOrigUid()).append("}) MATCH (d:").append(relation.getDestType()).append("{uid:")
                .append(relation.getDestUid()).append("}) USING INDEX d:").append(relation.getDestType())
                .append("(uid) MERGE (o)-[r:")
                .append(StringUtils.join(relation.getTags(), ":"));
        if (ListUtils.isNotEmpty(props)) {
                statementTemplate.append(propsJoined);
        }
        statementTemplate.append("]->(d)");

        //.append(" RETURN ID(r) AS UID");

        // Create the relationship
        StatementResult ret = tx.run(statementTemplate.toString());
        //relation.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    //-------------------------------------------------------------------------
    // C O N F I G U R A T I O N     N O D E     M A N A G E M E N T
    //-------------------------------------------------------------------------

    private boolean existConfigNode() {
        Session session = this.driver.session();
        StatementResult statementResult = session.run("match (n:" + Node.Type.CONFIG + "{uid:0}) return count(n) as count");
        session.close();

        return (statementResult.peek().get(0).asInt() == 1);
    }

    public long getUidCounter() {
        Session session = this.driver.session();
        StatementResult statementResult = session.run("match (n{uid:0}) return n." + PREFIX_ATTRIBUTES + "uidCounter");
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
            tx.success();
        }
        session.close();
    }

    private void updateConfigNode(ObjectMap attrs) {
        // Build Cypher statement
        StringBuilder cypher = new StringBuilder();
        cypher.append("merge (n{uid:0}) set ");
        List<String> sets = new ArrayList<>();
        for (String key: attrs.keySet()) {
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
            tx.success();
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
                    StatementResult ret = tx.run(cypher, params);
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
                tx.success();
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

                    StatementResult ret = tx.run(cypher.toString(), Values.parameters("toto", "hello"));
                    //long retUid = ret.peek().get("uid").asLong();
                }
                tx.success();
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
                    StatementResult ret = tx.run(cypher.toString());

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
                tx.success();
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

    private String cleanValue(String value) {
        return value.replace("\"", ",").replace("\\", "|");
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public Driver getDriver() {
        return driver;
    }

    public Neo4JNetworkDBAdaptor setDriver(Driver driver) {
        this.driver = driver;
        return this;
    }
}
