package org.opencb.bionetdb.app.cli.main;

import com.beust.jcommander.ParameterException;
import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.main.executors.ExpressionCommandExecutor;
import org.opencb.bionetdb.app.cli.main.executors.QueryCommandExecutor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;

import java.io.IOException;

/**
 * Created by imedina on 05/08/15.
 */
public class BioNetDBMain {
    public static final String VERSION = "1.0.0";

    public static void main(String[] args) {

        BioNetDBCliOptionsParser bioNetDBCliOptionsParser = new BioNetDBCliOptionsParser();
        try {
            bioNetDBCliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            bioNetDBCliOptionsParser.printUsage();
            System.exit(1);
        }

        String parsedCommand = bioNetDBCliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (bioNetDBCliOptionsParser.getGeneralOptions().help) {
                bioNetDBCliOptionsParser.printUsage();
                System.exit(0);
            } else {
                if (bioNetDBCliOptionsParser.getGeneralOptions().version) {
                    System.out.println("Version " + VERSION);
                    System.exit(0);
                } else {
                    bioNetDBCliOptionsParser.printUsage();
                    System.exit(1);
                }
            }
        } else {
            CommandExecutor commandExecutor = null;
            if (bioNetDBCliOptionsParser.isHelp()) {
                bioNetDBCliOptionsParser.printUsage();
                System.exit(0);
            } else {
                switch (parsedCommand) {
                    case "query":
                        commandExecutor = new QueryCommandExecutor(bioNetDBCliOptionsParser.getQueryCommandOptions());
                        break;
                    case "expression":
                        commandExecutor = new ExpressionCommandExecutor(bioNetDBCliOptionsParser.getExpressionCommandOptions());
                        break;
                    default:
                        break;
                }
            }

            if (commandExecutor != null) {
                try {
                    commandExecutor.loadBioNetDBConfiguration();
                    commandExecutor.execute();
                } catch (IOException e) {
                    commandExecutor.getLogger().error("Error reading CellBase configuration: " + e.getMessage());
                    System.exit(1);
                } catch (BioNetDBException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }
}
