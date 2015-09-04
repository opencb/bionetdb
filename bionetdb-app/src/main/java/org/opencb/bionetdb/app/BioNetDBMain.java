package org.opencb.bionetdb.app;

import com.beust.jcommander.ParameterException;
import org.opencb.bionetdb.app.cli.*;

/**
 * Created by imedina on 05/08/15.
 */
public class BioNetDBMain {
    public static final String VERSION = "1.0.0";

    public static void main(String[] args) {

        CliOptionsParser cliOptionsParser = new CliOptionsParser();
        try {
            cliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            cliOptionsParser.printUsage();
            System.exit(1);
        }

        String parsedCommand = cliOptionsParser.getCommand();
        if(parsedCommand == null || parsedCommand.isEmpty()) {
            if(cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                System.exit(0);
            } else {
                if(cliOptionsParser.getGeneralOptions().version) {
                    System.out.println("Version " + VERSION);
                    System.exit(0);
                } else {
                    cliOptionsParser.printUsage();
                    System.exit(1);
                }
            }
        } else {
            CommandExecutor commandExecutor = null;
            if(cliOptionsParser.isHelp()) {
                cliOptionsParser.printUsage();
                System.exit(0);
            } else {
                switch (parsedCommand) {
                    case "build":
                        commandExecutor = new BuildCommandExecutor(cliOptionsParser.getBuildCommandOptions());
                        break;
                    case "load":
                        commandExecutor = new LoadCommandExecutor(cliOptionsParser.getLoadCommandOptions());
                        break;
//                    case "query":
//                        commandExecutor = new QueryCommandExecutor(cliOptionsParser.getQueryCommandOptions());
//                        break;
                    case "annotation":
                        commandExecutor = new AnnotateCommandExecutor(cliOptionsParser.getVariantAnnotationCommandOptions());
                        break;
                    case "expression":
                        commandExecutor = new ExpressionCommandExecutor(cliOptionsParser.getExpressionCommandOptions());
                        break;
                    default:
                        break;
                }
            }

            if (commandExecutor != null) {
                commandExecutor.execute();
//                try {
//                    commandExecutor.loadCellBaseConfiguration();
//                    commandExecutor.execute();
//                } catch (IOException | URISyntaxException e) {
//                    commandExecutor.getLogger().error("Error reading CellBase configuration: " + e.getMessage());
//                    System.exit(1);
//                }
            }
        }
    }
}
