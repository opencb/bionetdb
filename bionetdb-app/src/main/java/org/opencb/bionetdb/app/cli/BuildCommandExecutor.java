package org.opencb.bionetdb.app.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.io.SbmlParser;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
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

            Network network = null;
            if (inputPath.getFileName().toString().endsWith("owl") || inputPath.getFileName().toString().endsWith("owl.gz")) {
                BioPaxParser bioPaxParser = new BioPaxParser("L3");
                network = bioPaxParser.parse(inputPath);
            } else {
                if (inputPath.getFileName().toString().endsWith("sbml") || inputPath.getFileName().toString().endsWith("sbml.gz")) {
                    SbmlParser sbmlParser = new SbmlParser();
                    network = sbmlParser.parse(inputPath);
                }
            }

            if (network != null) {
                // Print to file
                Path outputPath;
                if (buildCommandOptions.output == null || buildCommandOptions.output.isEmpty()) {
                    outputPath = Paths.get(buildCommandOptions.input+".json");
                } else {
                    outputPath = Paths.get(buildCommandOptions.output);
                }

                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputPath.toAbsolutePath().toString()));
                bufferedWriter.write(objectMapper.writeValueAsString(network));
                bufferedWriter.close();
            } else {
                logger.error("File extension has not been recognized, it must contain 'owl' or 'sbml', file name is '{}'", inputPath.getFileName());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
