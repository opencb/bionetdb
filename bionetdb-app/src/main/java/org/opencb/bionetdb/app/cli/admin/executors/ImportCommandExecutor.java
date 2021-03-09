package org.opencb.bionetdb.app.cli.admin.executors;

import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.admin.AdminCliOptionsParser;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.bionetdb.lib.utils.Importer;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

/**
 * Created by jtarraga on 21/06/18.
 */
public class ImportCommandExecutor extends CommandExecutor {

    private AdminCliOptionsParser.ImportCommandOptions importCommandOptions;

    public ImportCommandExecutor(AdminCliOptionsParser.ImportCommandOptions importCommandOptions) {
        super(importCommandOptions.commonOptions.logLevel, importCommandOptions.commonOptions.conf);

        this.importCommandOptions = importCommandOptions;
    }

    @Override
    public void execute() {
        try {
            // Check input directory
            Path inputPath = Paths.get(importCommandOptions.input);
            FileUtils.checkDirectory(inputPath);

            BioNetDbManager manager = new BioNetDbManager(configuration);
            manager.load(importCommandOptions.database, inputPath);
        } catch (IOException | BioNetDBException e) {
            e.printStackTrace();
        }
    }
}
