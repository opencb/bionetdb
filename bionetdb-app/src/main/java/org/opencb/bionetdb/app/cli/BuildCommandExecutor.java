package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 05/08/15.
 */
public class BuildCommandExecutor extends CommandExecutor {

    private CliOptionsParser.BuildCommandOptions buildCommandOptions;

    public BuildCommandExecutor(CliOptionsParser.BuildCommandOptions buildCommandOptions) {
        super(buildCommandOptions.commonOptions.logLevel, buildCommandOptions.commonOptions.conf);

        this.buildCommandOptions = buildCommandOptions;
    }

    @Override
    public void execute() {

        try {
            Path inputPath = Paths.get(buildCommandOptions.input);
            FileUtils.checkFile(inputPath);

            BioPaxParser bioPaxParser = new BioPaxParser("L3");
            bioPaxParser.parse(inputPath);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
