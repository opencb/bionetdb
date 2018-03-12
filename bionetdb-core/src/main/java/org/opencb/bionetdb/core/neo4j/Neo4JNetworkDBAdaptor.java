package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.NodeIterator;
import org.opencb.bionetdb.core.api.PathIterator;
import org.opencb.bionetdb.core.api.RowIterator;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.api.query.PathQuery;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.Neo4JConverter;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.cellbase.client.rest.VariationClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;

import java.io.IOException;
import java.util.*;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private Driver driver;

    private BioNetDBConfiguration configuration;
    private CellBaseClient cellBaseClient;

    public static final String PREFIX_ATTRIBUTES = "attr_";

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration) throws BioNetDBException {
        this(database, configuration, false);
    }

    public Neo4JNetworkDBAdaptor(String database, BioNetDBConfiguration configuration, boolean createIndex) throws BioNetDBException {
        this.configuration = configuration;

        DatabaseConfiguration databaseConfiguration = getDatabaseConfiguration(database);
        if (databaseConfiguration == null) {
            throw new BioNetDBException("No database found with name: \"" + database + "\"");
        }
        String databaseURI = databaseConfiguration.getHost() + ":" + databaseConfiguration.getPort();
        String user = databaseConfiguration.getUser();
        String password = databaseConfiguration.getPassword();

        driver = GraphDatabase.driver("bolt://" + databaseURI, AuthTokens.basic(user, password));
//        session = driver.session();

//        registerShutdownHook(this.driver, this.session);
        registerShutdownHook(this.driver);

        if (createIndex) {
            createIndexes();
        }
    }

    private void registerShutdownHook(final Driver driver) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> driver.close()));
    }

    private DatabaseConfiguration getDatabaseConfiguration(String database) {
        // configuration class checks if database is null or empty
        return configuration.findDatabase(database);
    }

    private void createIndexes() {
        Session session = this.driver.session();
        if (session != null) {
            try (Transaction tx = session.beginTransaction()) {
//                tx.run("CREATE INDEX ON :" + Node.Type.PHYSICAL_ENTITY + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.COMPLEX + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SMALL_MOLECULE + "(id)");
                //tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(uid)");
                tx.run("CREATE CONSTRAINT ON (cl:" + Node.Type.CELLULAR_LOCATION + ") assert cl.uid is unique");
                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(name)");
//                tx.run("CREATE INDEX ON :" + Node.Type.CATALYSIS + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.REACTION + "(id)");
                //tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(uid)");
                tx.run("CREATE CONSTRAINT ON (x:" + Node.Type.XREF + ") assert x.uid is unique");
                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(id,dbName)");
//                tx.run("CREATE INDEX ON :" + Node.Type.ONTOLOGY + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.UNDEFINED + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_CALL + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_FILE_INFO + "(id)");
//                tx.run("CREATE INDEX ON :" + Node.Type.SAMPLE + "(id)");
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
    public PathIterator pathIterator(PathQuery pathQuery, QueryOptions queryOptions) throws BioNetDBException {

        String cypher = Neo4JQueryParser.parsePath(pathQuery, queryOptions);
        return pathIterator(cypher);
    }

    @Override
    public PathIterator pathIterator(String cypher) throws BioNetDBException {
        Session session = this.driver.session();
//        System.out.println("Cypher query: " + cypher);
        return new Neo4JPathIterator(session.run(cypher));
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
    public QueryResult<Network> networkQueryByPaths(List<PathQuery> pathQueries, QueryOptions queryOptions) throws BioNetDBException {
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
        boolean prefix = false;
        List<String> props = new ArrayList<>();
        if (StringUtils.isNotEmpty(node.getId())) {
            props.add("n.id=\"" + cleanValue(node.getId()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getName())) {
            props.add("n.name=\"" + cleanValue(node.getName()) + "\"");
        }
        if (StringUtils.isNotEmpty(node.getSource())) {
            props.add("n.source=\"" + node.getSource() + "\"");
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
        cypher.append(" RETURN ID(n) AS UID");
        StatementResult ret = tx.run(cypher.toString());
        node.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    public StatementResult mergeNode(Node node, String byKey, Transaction tx) {
        // Gather properties of the node to create a cypher string with them
        boolean prefix = false;
        Object value = null;

        List<String> props = new ArrayList<>();
        if ("id".equals(byKey)) {
            value = node.getId();
        } else {
            if (StringUtils.isNotEmpty(node.getId())) {
                props.add("n.id=\"" + cleanValue(node.getId()) + "\"");
            }
        }
        if ("name".equals(byKey)) {
            value = node.getName();
        } else {
            if (StringUtils.isNotEmpty(node.getName())) {
                props.add("n.name=\"" + cleanValue(node.getName()) + "\"");
            }
        }
        if ("source".equals(byKey)) {
            value = node.getSource();
        } else {
            if (StringUtils.isNotEmpty(node.getSource())) {
                props.add("n.source=\"" + node.getSource() + "\"");
            }
        }
        for (String key: node.getAttributes().keySet()) {
            if (key.equals(byKey)) {
                prefix = true;
                value = node.getAttributes().get(key);
            } else {
                if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                    props.add("n." + PREFIX_ATTRIBUTES + key + "=" + node.getAttributes().getString(key));
                } else {
                    props.add("n." + PREFIX_ATTRIBUTES + key + "=\"" + cleanValue(node.getAttributes().getString(key)) + "\"");
                }
            }
        }
        //String propsJoined = "{" + String.join(",", props) + "}";

        // Create the desired node
        StringBuilder cypher = new StringBuilder("MERGE (n");
        if (ListUtils.isNotEmpty(node.getTags())) {
            cypher.append(":").append(StringUtils.join(node.getTags(), ":"));
        }
        cypher.append("{");
        if (prefix) {
            cypher.append(PREFIX_ATTRIBUTES);
        }
        cypher.append(byKey).append(":");
        if (value instanceof String) {
            cypher.append("\"").append(value).append("\"");
        } else {
            cypher.append(value);
        }
        cypher.append("})");
        if (ListUtils.isNotEmpty(props)) {
            cypher.append(" SET ").append(StringUtils.join(props, ","));
        }
        cypher.append(" RETURN ID(n) AS UID");
        StatementResult ret = tx.run(cypher.toString());
        node.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    public StatementResult mergeNode(Node node, String byKey1, String byKey2, Transaction tx) {
        // Gather properties of the node to create a cypher string with them
        boolean prefix1 = false;
        boolean prefix2 = false;
        Object value1 = null;
        Object value2 = null;

        List<String> props = new ArrayList<>();
        if ("id".equals(byKey1)) {
            value1 = node.getId();
        } else if ("id".equals(byKey2)) {
            value2 = node.getId();
        } else {
            if (StringUtils.isNotEmpty(node.getId())) {
                props.add("n.id=\"" + cleanValue(node.getId()) + "\"");
            }
        }
        if ("name".equals(byKey1)) {
            value1 = node.getName();
        } else if ("name".equals(byKey2)) {
            value2 = node.getName();
        } else {
            if (StringUtils.isNotEmpty(node.getName())) {
                props.add("n.name=\"" + cleanValue(node.getName()) + "\"");
            }
        }
        if ("source".equals(byKey1)) {
            value1 = node.getSource();
        } else if ("source".equals(byKey2)) {
            value2 = node.getSource();
        } else if (StringUtils.isNotEmpty(node.getSource())) {
            props.add("n.source=\"" + node.getSource() + "\"");
        }
        for (String key: node.getAttributes().keySet()) {
            if (key.equals(byKey1)) {
                prefix1 = true;
                value1 = node.getAttributes().get(key);
            } else if (key.equals(byKey2)) {
                prefix2 = true;
                value2 = node.getAttributes().get(key);
            } else {
                if (StringUtils.isNumeric(node.getAttributes().getString(key))) {
                    props.add("n." + PREFIX_ATTRIBUTES + key + "=" + node.getAttributes().getString(key));
                } else {
                    props.add("n." + PREFIX_ATTRIBUTES + key + "=\"" + cleanValue(node.getAttributes().getString(key)) + "\"");
                }
            }
        }
        //String propsJoined = "{" + String.join(",", props) + "}";

        // Create the desired node
        StringBuilder cypher = new StringBuilder("MERGE (n");
        if (ListUtils.isNotEmpty(node.getTags())) {
            cypher.append(":").append(StringUtils.join(node.getTags(), ":"));
        }
        cypher.append("{");
        if (prefix1) {
            cypher.append(PREFIX_ATTRIBUTES);
        }
        cypher.append(byKey1).append(":");
        if (value1 instanceof String) {
            cypher.append("\"").append(value1).append("\"");
        } else {
            cypher.append(value1);
        }
        cypher.append(",");
        if (prefix2) {
            cypher.append(PREFIX_ATTRIBUTES);
        }
        cypher.append(byKey2).append(":");
        if (value1 instanceof String) {
            cypher.append("\"").append(value2).append("\"");
        } else {
            cypher.append(value2);
        }
        cypher.append("})");
        if (ListUtils.isNotEmpty(props)) {
            cypher.append(" SET ").append(StringUtils.join(props, ","));
        }
        cypher.append(" RETURN ID(n) AS UID");
        StatementResult ret = tx.run(cypher.toString());
        node.setUid(ret.peek().get("UID").asLong());
        return ret;
    }

    public StatementResult mergeRelation(Relation relation, Transaction tx) {
        return addRelation(relation, tx);
    }

    public StatementResult addRelation(Relation relation, Transaction tx) {
        List<String> props = new ArrayList<>();
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
        statementTemplate.append("MATCH (o:$origType) WHERE ID(o) = $origUid")
                .append(" MATCH (d:$destType) WHERE ID(d) = $destUid")
                .append(" MERGE (o)-[r:").append(StringUtils.join(relation.getTags(), ":")).append(propsJoined).append("]->(d)")
                .append(" RETURN ID(r) AS UID");

        // Create the relationship
        StatementResult ret = tx.run(statementTemplate.toString(),
                parameters("origType", relation.getOrigType(), "origUid", relation.getOrigUid(),
                        "destType", relation.getDestType(), "destUid", relation.getDestUid()));
        relation.setUid(ret.peek().get("UID").asLong());
        return ret;
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
