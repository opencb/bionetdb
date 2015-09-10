package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.io.ExpressionParser;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 4/9/15.
 */
public class ExpressionCommandExecutor extends CommandExecutor {

    private CliOptionsParser.ExpressionCommandOptions expressionCommandOptions;

    public ExpressionCommandExecutor(CliOptionsParser.ExpressionCommandOptions expressionCommandOptions) {
        super(expressionCommandOptions.commonOptions.logLevel, expressionCommandOptions.commonOptions.conf);

        this.expressionCommandOptions = expressionCommandOptions;
    }

    @Override
    public void execute() {
        // TODO: Contemplate import of just one expression file, in which case, the metadata file won't be necessary.
        try {
            Path metadata = Paths.get(expressionCommandOptions.metadata);
            FileUtils.checkFile(metadata);
            ExpressionParser expressionParser = new ExpressionParser(metadata);
            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(expressionCommandOptions.database);

            Map<String, Map<String, String>> allExpressionFiles = expressionParser.getMyFiles();
            for (String tissue : allExpressionFiles.keySet()) {
                for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                    if ((expressionCommandOptions.tissues == null || expressionCommandOptions.tissues.contains(tissue)) &&
                            (expressionCommandOptions.timeseries == null || expressionCommandOptions.timeseries.contains(timeseries))) {
                        List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                        networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, expressionCommandOptions.add);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}


