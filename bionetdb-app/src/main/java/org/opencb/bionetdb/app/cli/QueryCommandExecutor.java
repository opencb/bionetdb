package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.datastore.core.Query;

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
            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(queryCommandOptions.database);

            if (queryCommandOptions.betweenness) {
                Query query = new Query("id", queryCommandOptions.id);
                query.put("nodeLabel", queryCommandOptions.nodeType);

                networkDBAdaptor.betweenness(query);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
