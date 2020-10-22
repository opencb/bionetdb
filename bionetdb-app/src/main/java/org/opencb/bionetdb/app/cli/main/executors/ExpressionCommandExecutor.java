package org.opencb.bionetdb.app.cli.main.executors;

import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.main.BioNetDBCliOptionsParser;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.io.ExpressionParser;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.db.Neo4JNetworkDBAdaptor;
import org.opencb.commons.datastore.core.QueryOptions;
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

    private BioNetDBCliOptionsParser.ExpressionCommandOptions expressionCommandOptions;

    public ExpressionCommandExecutor(BioNetDBCliOptionsParser.ExpressionCommandOptions expressionCommandOptions) {
        super(expressionCommandOptions.commonOptions.logLevel, expressionCommandOptions.commonOptions.conf);

        this.expressionCommandOptions = expressionCommandOptions;
    }

    @Override
    public void execute() throws BioNetDBException {
        // TODO: Contemplate import of just one expression file, in which case, the metadata file won't be necessary.
        try {
            Path metadata = Paths.get(expressionCommandOptions.metadata);
            FileUtils.checkFile(metadata);
            ExpressionParser expressionParser = new ExpressionParser(metadata);
            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(configuration);

            QueryOptions options = new QueryOptions();
            options.put("addNodes", this.expressionCommandOptions.add);

            Map<String, Map<String, String>> allExpressionFiles = expressionParser.getMyFiles();
            for (String tissue : allExpressionFiles.keySet()) {
                for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                    if ((expressionCommandOptions.tissues == null || expressionCommandOptions.tissues.contains(tissue)) &&
                            (expressionCommandOptions.timeseries == null || expressionCommandOptions.timeseries.contains(timeseries))) {
                        List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                        //networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}


