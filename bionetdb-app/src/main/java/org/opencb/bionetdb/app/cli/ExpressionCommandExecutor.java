package org.opencb.bionetdb.app.cli;

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

    }


}


