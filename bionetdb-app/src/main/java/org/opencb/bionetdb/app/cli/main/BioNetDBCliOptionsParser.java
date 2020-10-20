package org.opencb.bionetdb.app.cli.main;

import com.beust.jcommander.*;
import org.opencb.bionetdb.app.cli.CliOptionsParser;

import java.util.List;

/**
 * Created by imedina on 05/08/15.
 */
public class BioNetDBCliOptionsParser extends CliOptionsParser {

    private final CommonCommandOptions commonCommandOptions;

    private QueryCommandOptions queryCommandOptions;
    private ExpressionCommandOptions expressionCommandOptions;

    public BioNetDBCliOptionsParser() {
        programName = "bionetdb.sh";
        jCommander.setProgramName(programName);

        commonCommandOptions = new CommonCommandOptions();

        queryCommandOptions = new QueryCommandOptions();
        expressionCommandOptions = new ExpressionCommandOptions();

        jCommander.addCommand("query", queryCommandOptions);
        jCommander.addCommand("expression",expressionCommandOptions);
    }

    public void parse(String[] args) throws ParameterException {
        jCommander.parse(args);
    }

    public String getCommand() {
        return (jCommander.getParsedCommand() != null) ? jCommander.getParsedCommand(): "";
    }

    public boolean isHelp() {
        String parsedCommand = jCommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jC = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jC.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof CommonCommandOptions) {
                return ((CommonCommandOptions) objects.get(0)).help;
            }
        }
        return getCommonCommandOptions().help;
    }

    public class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, description = "Display this help and exit", help = true)
        public boolean help;

        @Parameter(names = {"--version"}, description = "Display the version and exit")
        public boolean version;

    }

    public class CommonCommandOptions {

        @Parameter(names = {"-h", "--help"}, description = "Display this help and exit", help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "Set the logging level, accepted values are: debug, info, warn, error and fatal", required = false, arity = 1)
        public String logLevel = "info";

        @Parameter(names = {"-C", "--conf"}, description = "CellBase configuration.json file. Have a look at cellbase/cellbase-core/src/main/resources/configuration.json for an example", required = false, arity = 1)
        public String conf;

    }

    @Parameters(commandNames = {"query"}, commandDescription = "Query and fetch data from CellBase database using this command line")
    public class QueryCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "By physical entity ID", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--type"}, description = "PhysicalEntity, Interaction, Xref...", required = false, arity = 1)
        public String nodeType;

        @Parameter(names = {"--cellular-location"}, description = "cellular location", required = false, arity = 1)
        public String cellularLocation;

        @Parameter(names = {"--n"}, description = "Main node", required = false, arity = 1)
        public String n;

        @Parameter(names = {"--m"}, description = "Related node", required = false, arity = 1)
        public String m;

        @Parameter(names = {"--jumps"}, description = "Number of Jumps", required = false, arity = 1)
        public int jumps = 1;

        @Parameter(names = {"--relationship"}, description = "Relation", required = false, arity = 1)
        public String relationship;

        @Parameter(names = {"--database"}, description = "Data model type to be loaded, i.e. genome, gene, ...", required = false, arity = 1)
        public String database;

        @Parameter(names = {"--betweenness"}, description = "", required = false, arity = 0)
        public boolean betweenness;

        @Parameter(names = {"--clusteringCoeff"}, description = "", required = false, arity = 0)
        public boolean clusteringCoeff;

        @Parameter(names = {"-o", "--output-file"}, description = "", required = false, arity = 1)
        public String outputFile;

        @Parameter(names = {"--host"}, description = "...", required = false, arity = 1)
        public String host = "localhost";

        @Parameter(names = {"--port"}, description = "...", required = false, arity = 1)
        public int port = 7687;

        @Parameter(names = {"-u", "--user"}, description = "Data model type to be loaded, i.e. genome, gene, ...", required = false, arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", password = true, arity = 1)
        public String password;
    }

    @Parameters(commandNames = {"expression"}, commandDescription = "Include expression data into the database")
    public class ExpressionCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--database"}, description = "Path to the database", required = false, arity = 1)
        public String database;

        @Parameter(names = {"--metadata"}, description = "File containing metadata information of the expression data", required = false, arity = 1)
        public String metadata;

        @Parameter(names = {"--tissue"}, description = "Tissues to be imported from the metadata file separated by commas. If left empty, all the tissues will be considered.", required = false, arity = 1)
        public List<String> tissues;

        @Parameter(names = {"--timeseries"}, description = "Timeseries to be imported from the metadata file separated by commas. If left empty, all the tissues will be considered.", required = false, arity = 1)
        public List<String> timeseries;

        @Parameter(names = {"--add"}, description = "Create nodes with IDs not found in the database. By default, they will be ignored.")
        public boolean add;

    }

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public QueryCommandOptions getQueryCommandOptions() {
        return queryCommandOptions;
    }

    public ExpressionCommandOptions getExpressionCommandOptions () { return expressionCommandOptions; }
}
