package org.opencb.bionetdb.core.neo4j;

import org.opencb.bionetdb.core.api.NetworkDBManager;
import org.opencb.bionetdb.core.api.NetworkIterator;
import org.opencb.bionetdb.core.api.NetworkManager;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Node;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class Neo4JNetworkDBManager implements NetworkDBManager {

    private String database;
    private BioNetDBConfiguration bioNetDBConfiguration;
    private Neo4JNetworkDBAdaptor networkDBAdaptor;

    private Logger logger;

    public Neo4JNetworkDBManager(String database, BioNetDBConfiguration bioNetDBConfiguration) {
        this.database = database;
        this.bioNetDBConfiguration = bioNetDBConfiguration;

        logger = LoggerFactory.getLogger(Neo4JNetworkDBManager.class);
    }

    private void init() throws BioNetDBException {
        if (networkDBAdaptor == null) {
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration, true);
        }
    }

    @Override
    public void load(Path path) throws IOException, BioNetDBException {
        load(path, null);
    }

    @Override
    public void load(Path path, QueryOptions queryOptions) throws IOException, BioNetDBException {
        init();

        // Neo4J Manager parses and loads BioPax files
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Network network = bioPaxParser.parse(path);
        logger.info("The file '{}' has been parsed.", path);

        // Inserting data
        logger.info("Inserting data...");
        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, queryOptions);
        long stopTime = System.currentTimeMillis();
        logger.info("Done. Data insertion took " + (stopTime - startTime) / 1000 + " seconds.");

        // Get statistics
        QueryResult myResult = getSummaryStats(null, null);
        logger.info("Number of nodes: {}", ((ObjectMap) myResult.getResult().get(0)).getInt("totalNodes"));
        logger.info("Number of relationships: {}", ((ObjectMap) myResult.getResult().get(0)).getInt("totalRelations"));
    }

    @Override
    public QueryResult<Node> query(Query query, QueryOptions queryOptions) throws BioNetDBException {
        init();

        return null;
        //return networkDBAdaptor.query(query, queryOptions);
    }

    @Override
    public NetworkIterator iterator(Query query, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public void annotate() {

    }

    @Override
    public void annotateGenes(Query query, QueryOptions queryOptions) {

    }

    @Override
    public void annotateVariants(Query query, QueryOptions queryOptions) {

    }

    @Override
    public QueryResult getSummaryStats(Query query, QueryOptions queryOptions) throws BioNetDBException {
        init();

        return networkDBAdaptor.getSummaryStats(query, queryOptions);
    }

    @Override
    public void close() throws Exception {
        if (networkDBAdaptor != null) {
            networkDBAdaptor.close();
        }
    }
}
