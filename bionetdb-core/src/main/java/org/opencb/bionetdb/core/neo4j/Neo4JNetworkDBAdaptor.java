package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
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
import org.opencb.bionetdb.core.models.PhysicalEntity;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.Neo4JConverter;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.cellbase.client.rest.VariationClient;
import org.opencb.commons.datastore.core.Query;
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
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                driver.close();
            }
        });
    }

    private DatabaseConfiguration getDatabaseConfiguration(String database) {
        DatabaseConfiguration databaseConfiguration;
        if (database != null && !database.isEmpty()) {
            databaseConfiguration = configuration.findDatabase(database);
        } else {
            databaseConfiguration = configuration.findDatabase();
        }
        return databaseConfiguration;
    }

    private void createIndexes() {
        Session session = this.driver.session();
        if (session != null) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :" + Node.Type.PHYSICAL_ENTITY + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.PROTEIN + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.COMPLEX + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SMALL_MOLECULE + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CELLULAR_LOCATION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.CATALYSIS + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.REACTION + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.XREF + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.ONTOLOGY + "(uid)");
                tx.run("CREATE INDEX ON :" + PhysicalEntity.Type.UNDEFINED + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_CALL + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.VARIANT_FILE_INFO + "(uid)");
                tx.run("CREATE INDEX ON :" + Node.Type.SAMPLE + "(uid)");
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

    public void annotateProtein(ProteinClient proteinClient) throws BioNetDBException, IOException {
        // First, get all proteins from the network
        Query query = new Query();
        String cypher = "MATCH path=(p:PROTEIN)-[xr:XREF]->(x:XREF) WHERE toLower(x.attr_source) = \"uniprot\" return path";
        QueryResult<Network> networkResult = networkQuery(cypher);

        if (ListUtils.isEmpty(networkResult.getResult())) {
            System.out.println("Network not found!!");
            return;
        }
        Network network = networkResult.getResult().get(0);

        // Get proteins annotations from Cellbase...
        // ... prepare list of protein id/names from xref/protein nodes
        List<String> proteinIds = new ArrayList<>();
        for (Node node: network.getNodes()) {
            if (node.getType() == Node.Type.XREF) {
                proteinIds.add(node.getId());
            }
        }

        // ... finally, call Cellbase service
        Map<String, Entry> proteinMap = new HashMap<>();
        QueryResponse<Entry> entryQueryResponse = proteinClient.get(proteinIds, new QueryOptions(QueryOptions.EXCLUDE,
                "reference,organism,comment,evidence,sequence"));
        for (QueryResult<Entry> queryResult: entryQueryResponse.getResponse()) {
            proteinMap.put(queryResult.getId(), queryResult.getResult().get(0));
        }

        for (String key: proteinMap.keySet()) {
            System.out.println(key + " -> " + proteinMap.get(key));
        }
    }

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
        statementTemplate.append("MATCH (o) WHERE ID(o) = $origUid")
                .append(" MATCH (d) WHERE ID(d) = $destUid")
                .append(" MERGE (o)-[r:").append(StringUtils.join(relation.getTags(), ":")).append(propsJoined).append("]->(d)")
                .append(" RETURN ID(r) AS UID");

        // Create the relationship
        StatementResult ret = tx.run(statementTemplate.toString(),
                parameters("origUid", relation.getOrigUid(), "destUid", relation.getDestUid()));
        relation.setUid(ret.peek().get("UID").asLong());
        return ret;
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
