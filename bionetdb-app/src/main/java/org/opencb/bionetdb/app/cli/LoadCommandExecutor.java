package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

            // BioNetDbManager checks if database parameter is empty
            BioNetDbManager bioNetDbManager = new BioNetDbManager(loadCommandOptions.database, configuration);

            Map<String, Set<String>> filter = null;
            if (ListUtils.isNotEmpty(loadCommandOptions.exclude)) {
                filter = new HashMap<>();
                for (String exclude: loadCommandOptions.exclude) {
                    String split[] = exclude.split(":");
                    if (split.length == 2) {
                        if (!filter.containsKey(split[0])) {
                            filter.put(split[0], new HashSet<>());
                        }
                        filter.get(split[0]).add(split[1]);
                    }
                }
            }
            bioNetDbManager.loadBioPax(inputPath, filter);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
