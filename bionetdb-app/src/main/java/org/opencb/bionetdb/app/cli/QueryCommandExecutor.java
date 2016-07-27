package org.opencb.bionetdb.app.cli;

import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 28/09/15.
 */
public class QueryCommandExecutor extends CommandExecutor {

    private CliOptionsParser.QueryCommandOptions queryCommandOptions;

    public QueryCommandExecutor(CliOptionsParser.QueryCommandOptions queryCommandOptions) {
        super(queryCommandOptions.commonOptions.logLevel, queryCommandOptions.commonOptions.conf);

        this.queryCommandOptions = queryCommandOptions;
    }

    @Override
    public void execute() {

        try {
            if (queryCommandOptions.database == null || queryCommandOptions.database.isEmpty()) {
                if (queryCommandOptions.database == null || queryCommandOptions.database.isEmpty()) {
                    DatabaseConfiguration databaseConfiguration = createDatabaseConfigurationFromCLI("unknown", queryCommandOptions.host,
                            queryCommandOptions.port, queryCommandOptions.user, queryCommandOptions.password);
                    configuration.getDatabases().add(databaseConfiguration);
                }
            }

            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor("unknown", configuration);

            if (queryCommandOptions.betweenness) {
                Query query = new Query("id", queryCommandOptions.id);
//                query.put("nodeLabel", queryCommandOptions.nodeType);
                query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), queryCommandOptions.nodeType);

                QueryResult betweenness = networkDBAdaptor.betweenness(query);
                System.out.println("betweenness = " + betweenness);
                return;
            }

            if (queryCommandOptions.clusteringCoeff) {
                Query query = new Query("id", queryCommandOptions.id);
                query.put("nodeLabel", queryCommandOptions.nodeType);

                QueryResult queryResult = networkDBAdaptor.clusteringCoefficient(query);
                System.out.println("queryResult = " + queryResult);
            }

            Query query = new Query();
            if (StringUtils.isNotEmpty(queryCommandOptions.id)) {
//                Query query = new Query(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), queryCommandOptions.id);
                query.put(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), queryCommandOptions.id);

            }

            if (StringUtils.isNotEmpty(queryCommandOptions.nodeType)) {
                query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), queryCommandOptions.nodeType);
            }

            if (StringUtils.isNotEmpty(queryCommandOptions.cellularLocation)) {
                query.put(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key(), queryCommandOptions.cellularLocation);
            }
//            networkDBAdaptor.getNodes(query, null);

            if (StringUtils.isNotEmpty(queryCommandOptions.n) && StringUtils.isNotEmpty(queryCommandOptions.m)) {
                Query query1 = buildNodeQuery(queryCommandOptions.n);
                Query query2 = buildNodeQuery(queryCommandOptions.m);
                QueryOptions queryOptions = new QueryOptions();
                if (queryCommandOptions.jumps != 0) {
                    queryOptions.put(NetworkDBAdaptor.NetworkQueryParams.JUMPS.key(), queryCommandOptions.jumps);
                } else {
                    queryOptions.put(NetworkDBAdaptor.NetworkQueryParams.JUMPS.key(), 1);
                }
                if (StringUtils.isNotEmpty(queryCommandOptions.relationship)) {
                    queryOptions.put(NetworkDBAdaptor.NetworkQueryParams.REL_TYPE.key(), queryCommandOptions.relationship);
                }
                networkDBAdaptor.getNodes(query1, query2, queryOptions);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private Query buildNodeQuery(String node) {
        Query query = new Query();
        List<String> properties = Arrays.asList(node.split(","));
        for (String item: properties) {
            String[] items = item.split(":");
            switch(items[0]) {
                case "id":
                    query.put(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), items[1]);
                    break;
                case "cl":
                    query.put(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key(), items[1]);
                    break;
                case "on":
                    query.put(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key(), items[1]);
                    break;
            }
        }
        return query;
    }

}
