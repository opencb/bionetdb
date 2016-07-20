package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.commons.utils.FileUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

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

        try {
            Path inputPath = Paths.get(loadCommandOptions.input);
            FileUtils.checkFile(inputPath);
            BioPaxParser bioPaxParser = new BioPaxParser("L3");
            Network network = bioPaxParser.parse(inputPath);

            if (loadCommandOptions.database == null || loadCommandOptions.database.isEmpty()) {
                loadCommandOptions.database = "unknown";

                DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration(loadCommandOptions.database, null);
                databaseConfiguration.setHost(loadCommandOptions.host);
                databaseConfiguration.setPort(loadCommandOptions.port);
                databaseConfiguration.setUser(loadCommandOptions.user);
                databaseConfiguration.setPassword(loadCommandOptions.password);

                configuration.getDatabases().add(databaseConfiguration);
            }

            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(loadCommandOptions.database, configuration, true);
            networkDBAdaptor.insert(network, null);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
