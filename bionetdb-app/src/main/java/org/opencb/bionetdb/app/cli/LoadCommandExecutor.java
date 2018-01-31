package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.network.Network;
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

//            ObjectMapper objectMapper = new ObjectMapper();
//            ObjectReader objectReader = objectMapper.readerFor(Network.class);
//            Network network = objectReader.readValue(inputPath.toFile());


            // From biopax
            BioPaxParser bioPaxParser = new BioPaxParser("L3");
            Network network = bioPaxParser.parse(inputPath);

            String database = loadCommandOptions.database;;
            if (database == null || database.isEmpty()) {
                if (configuration.getDatabases() != null && configuration.getDatabases().size() > 0) {
                    database = configuration.getDatabases().get(0).getId();
                } else {
                    database = "unknown";
                    DatabaseConfiguration databaseConfiguration = createDatabaseConfigurationFromCLI("unknown",
                            loadCommandOptions.host, loadCommandOptions.port, loadCommandOptions.user, loadCommandOptions.password);
                    configuration.getDatabases().add(databaseConfiguration);
                }
            }

            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, configuration, true);
            networkDBAdaptor.insert(network, null);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
