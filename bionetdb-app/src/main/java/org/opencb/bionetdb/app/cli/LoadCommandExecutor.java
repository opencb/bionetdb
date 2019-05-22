package org.opencb.bionetdb.app.cli;

import org.apache.commons.lang.StringUtils;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.Neo4JLoader;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by imedina on 12/08/15.
 */
public class LoadCommandExecutor extends CommandExecutor {

    private final String CLINICAL_ANALYSIS = "clinical-analysis";
    private final Set<String> dataTypes = new HashSet<>(Arrays.asList(CLINICAL_ANALYSIS));

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

            // BioNetDbManager checks if database parameter is empty
            BioNetDbManager bioNetDbManager = new BioNetDbManager(loadCommandOptions.database, configuration);

            if (dataTypes.contains(loadCommandOptions.dataType)) {
                if (CLINICAL_ANALYSIS.equals(loadCommandOptions.dataType)) {
                    bioNetDbManager.loadClinicalAnalysis(inputPath);
                }
            } else {
                throw new BioNetDBException("Unknown data type to load: " + loadCommandOptions.dataType
                        + ". Valid data types values are: " + StringUtils.join(dataTypes, ","));
            }

//            Map<String, Set<String>> filter = null;
//            if (ListUtils.isNotEmpty(loadCommandOptions.exclude)) {
//                filter = new HashMap<>();
//                for (String exclude: loadCommandOptions.exclude) {
//                    String split[] = exclude.split(":");
//                    if (split.length == 2) {
//                        if (!filter.containsKey(split[0])) {
//                            filter.put(split[0], new HashSet<>());
//                        }
//                        filter.get(split[0]).add(split[1]);
//                    }
//                }
//            }
//            bioNetDbManager.loadBioPax(inputPath, filter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
