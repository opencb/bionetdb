package org.opencb.bionetdb.app.cli.main.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.main.BioNetDBCliOptionsParser;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.lib.db.Neo4JNetworkDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 28/09/15.
 */
public class QueryCommandExecutor extends CommandExecutor {

    private BioNetDBCliOptionsParser.QueryCommandOptions queryCommandOptions;

    public QueryCommandExecutor(BioNetDBCliOptionsParser.QueryCommandOptions queryCommandOptions) {
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

                QueryResult betweenness = null; //networkDBAdaptor.betweenness(query);
                System.out.println("betweenness = " + betweenness);
                return;
            }

            if (queryCommandOptions.clusteringCoeff) {
                Query query = new Query("id", queryCommandOptions.id);
                query.put("nodeLabel", queryCommandOptions.nodeType);

                QueryResult queryResult = null; //networkDBAdaptor.clusteringCoefficient(query);
                System.out.println("queryResult = " + queryResult);
                return;
            }

            // CLI query example:
            // query --id P40343 --cellular-location cytosol --m "id=5732871;cl=Golgi membrane" --jumps 2
            Query query1 = new Query();
            if (StringUtils.isNotEmpty(queryCommandOptions.id)) {
                query1.put(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), queryCommandOptions.id);
            }

            if (StringUtils.isNotEmpty(queryCommandOptions.nodeType)) {
                query1.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), queryCommandOptions.nodeType);
            }

            if (StringUtils.isNotEmpty(queryCommandOptions.cellularLocation)) {
                query1.put(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key(), queryCommandOptions.cellularLocation);
            }

            if (StringUtils.isNotEmpty(queryCommandOptions.m)) {
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
                //networkDBAdaptor.getNodes(query1, query2, queryOptions);
                return;
            }
            //networkDBAdaptor.getNodes(query1, null);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private Query buildNodeQuery(String node) {
        Query query = new Query();
        List<String> properties = Arrays.asList(node.split(";"));
        for (String item: properties) {
            String[] items = item.split("=");
            switch(items[0]) {
                case "id":
                    query.put(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), items[1]);
                    break;
                case "ty":
                    query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), items[1]);
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
