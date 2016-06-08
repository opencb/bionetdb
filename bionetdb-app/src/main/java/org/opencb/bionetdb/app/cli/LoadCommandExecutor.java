package org.opencb.bionetdb.app.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
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

//            ObjectReader objectReader = new ObjectMapper().readerFor(Network.class);
//            Network network = objectReader.readValue(Files.newBufferedReader(inputPath));

            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(loadCommandOptions.database, configuration);

            networkDBAdaptor.insert(network, null);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
