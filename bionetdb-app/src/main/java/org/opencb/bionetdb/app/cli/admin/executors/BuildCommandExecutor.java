package org.opencb.bionetdb.app.cli.admin.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.admin.AdminCliOptionsParser;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.io.SbmlParser;
import org.opencb.bionetdb.core.io.SifParser;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.lib.utils.Builder;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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

            Builder builder = new Builder(inputPath, outputPath, parseFilters(buildCommandOptions.exclude));
            builder.build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Set<String>> parseFilters(List<String> excludeList) {
        Map<String, Set<String>> filters = null;
        if (CollectionUtils.isNotEmpty(excludeList)) {
            filters = new HashMap<>();
            for (String exclude: excludeList) {
                String split[] = exclude.split(":");
                if (split.length == 2) {
                    if (!filters.containsKey(split[0])) {
                        filters.put(split[0], new HashSet<>());
                    }
                    filters.get(split[0]).add(split[1]);
                }
            }
        }
        return filters;
    }
}
