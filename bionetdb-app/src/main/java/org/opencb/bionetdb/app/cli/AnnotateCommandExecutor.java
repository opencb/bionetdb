package org.opencb.bionetdb.app.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 24/08/15.
 */
public class AnnotateCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantAnnotationCommandOptions annotateCommandOptions;

    public AnnotateCommandExecutor(CliOptionsParser.VariantAnnotationCommandOptions annotateCommandOptions) {
        super(annotateCommandOptions.commonOptions.logLevel, annotateCommandOptions.commonOptions.conf);

        this.annotateCommandOptions = annotateCommandOptions;
    }

    @Override
    public void execute() {
        // System.out.println("HOLA ANNOTATE");

    }

}
