package org.opencb.bionetdb.app.cli.admin.executors;

import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.admin.AdminCliOptionsParser;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 05/08/15.
 */
public class BuildCommandExecutor extends CommandExecutor {

    private AdminCliOptionsParser.BuildCommandOptions buildCommandOptions;

    public BuildCommandExecutor(AdminCliOptionsParser.BuildCommandOptions buildCommandOptions) {
        super(buildCommandOptions.commonOptions.logLevel, buildCommandOptions.commonOptions.conf);

        this.buildCommandOptions = buildCommandOptions;
    }

    @Override
    public void execute() {
        try {
            // Check input and output directories
            Path inputPath = Paths.get(buildCommandOptions.input);
            FileUtils.checkDirectory(inputPath);

            Path outputPath = Paths.get(buildCommandOptions.output);
            FileUtils.checkDirectory(outputPath);

            BioNetDbManager manager = new BioNetDbManager(configuration);
            manager.build(inputPath, outputPath, buildCommandOptions.networkFiles, buildCommandOptions.exclude);
        } catch (IOException | BioNetDBException e) {
            e.printStackTrace();
        }
    }
}
