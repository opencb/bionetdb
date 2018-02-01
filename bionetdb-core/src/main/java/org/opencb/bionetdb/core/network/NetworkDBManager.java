package org.opencb.bionetdb.core.network;

import org.opencb.bionetdb.core.api.NetworkIterator;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by joaquin on 1/29/18.
 */
public class NetworkDBManager {

    private String database;
    private BioNetDBConfiguration bioNetDBConfiguration;
    private Neo4JNetworkDBAdaptor networkDBAdaptor;

    private Logger logger;

    public NetworkDBManager(String database, BioNetDBConfiguration bioNetDBConfiguration) {
        this.database = database;
        this.bioNetDBConfiguration = bioNetDBConfiguration;
        this.networkDBAdaptor = null;

        logger = LoggerFactory.getLogger(NetworkDBManager.class);
    }

    private void init() throws BioNetDBException {
        if (networkDBAdaptor == null) {
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration, true);
        }
    }

    public void loadBioPax(Path path) throws IOException, BioNetDBException {
        // Be sure to initialize the network DB adapter
        init();

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

    public Node getNode(long uid) throws BioNetDBException {
        return null;
    }

    public QueryResult<Node> query(Query query, QueryOptions queryOptions) throws BioNetDBException {
        return null;
    }

    public QueryResult<Node> query(String script) throws BioNetDBException {
        return null;
    }

    public NetworkIterator iterator(Query query, QueryOptions queryOptions) {
        return null;
    }

    public void annotate() {
    }

    public void annotateGenes(Query query, QueryOptions queryOptions) {

    }

    public void annotateVariants(Query query, QueryOptions queryOptions) {

    }

    public QueryResult getSummaryStats(Query query, QueryOptions queryOptions) throws BioNetDBException {
        return null;
    }
}
