package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.BioNetDBManager;
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
            if (configuration == null || configuration.getDatabases() == null || configuration.getDatabases().size() == 0) {
                logger.error("Configuration objects is null or databases are empty");
                return;
            }

            Path inputPath = Paths.get(loadCommandOptions.input);
            FileUtils.checkFile(inputPath);

            // BioNetDBManager checks if database parameter is empty
            BioNetDBManager bioNetDBManager = new BioNetDBManager(loadCommandOptions.database, configuration);
            bioNetDBManager.loadBioPax(inputPath);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
