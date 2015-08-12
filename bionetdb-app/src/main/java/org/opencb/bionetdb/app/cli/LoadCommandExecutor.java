package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;

/**
 * Created by imedina on 12/08/15.
 */
public class LoadCommandExecutor extends CommandExecutor {

    private CliOptionsParser.LoadCommandOptions loadCommandOptions;

    public LoadCommandExecutor(CliOptionsParser.LoadCommandOptions loadCommandOptions) {
        super(loadCommandOptions.commonOptions.logLevel, loadCommandOptions.commonOptions.conf);

        this.loadCommandOptions = loadCommandOptions;
    }

    @Override
    public void execute() {

        NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(loadCommandOptions.database);
        


    }
}
