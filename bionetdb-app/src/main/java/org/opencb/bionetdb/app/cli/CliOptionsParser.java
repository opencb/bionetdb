package org.opencb.bionetdb.app.cli;

import com.beust.jcommander.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 05/08/15.
 */
public class CliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonCommandOptions commonCommandOptions;

    private BuildCommandOptions buildCommandOptions;
    private LoadCommandOptions loadCommandOptions;
    private CreateCsvCommandOptions createCsvCommandOptions;
    private ImportCommandOptions importCommandOptions;
    private QueryCommandOptions queryCommandOptions;
    private VariantAnnotationCommandOptions variantAnnotationCommandOptions;
    private ExpressionCommandOptions expressionCommandOptions;


    public CliOptionsParser() {
        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);
        jcommander.setProgramName("bionetdb.sh");

        commonCommandOptions = new CommonCommandOptions();

        buildCommandOptions = new BuildCommandOptions();
        loadCommandOptions = new LoadCommandOptions();
        createCsvCommandOptions = new CreateCsvCommandOptions();
        importCommandOptions = new ImportCommandOptions();
        queryCommandOptions = new QueryCommandOptions();
        variantAnnotationCommandOptions = new VariantAnnotationCommandOptions();
        expressionCommandOptions = new ExpressionCommandOptions();

        jcommander.addCommand("build", buildCommandOptions);
        jcommander.addCommand("load", loadCommandOptions);
        jcommander.addCommand("create-csv", createCsvCommandOptions);
        jcommander.addCommand("import", importCommandOptions);
        jcommander.addCommand("query", queryCommandOptions);
        jcommander.addCommand("annotation", variantAnnotationCommandOptions);
        jcommander.addCommand("expression",expressionCommandOptions);

    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand(): "";
    }

    public boolean isHelp() {
        String parsedCommand = jcommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander = jcommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander.getObjects();
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


    @Parameters(commandNames = {"build"}, commandDescription = "Build CellBase data models from all data sources downloaded")
    public class BuildCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "Input file with the downloaded network", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output file where the data model is stored", required = false, arity = 1)
        public String output;

        @Parameter(names = {"-s", "--species"}, description = "Name of the species to be built, valid format include 'Homo sapiens' or 'hsapiens'", required = false, arity = 1)
        public String species = "Homo sapiens";

    }


    @Parameters(commandNames = {"load"}, commandDescription = "Load the built data models into the database")
    public class LoadCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input directory", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-d", "--data-type"}, description = "Data type. Valid values: clinical-analysis", required = true, arity = 1)
        public String dataType;

        @Parameter(names = {"--database"}, description = "Data model type to be loaded, i.e. genome, gene, ...", arity = 1)
        public String database;
//
//        @Parameter(names = {"--exclude"}, description = "Exclude information separated by comma, e.g.:'XREF_DBNAME:Reactome Database ID Release 63'", arity = 1)
//        public List<String> exclude;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        public Map<String, String> loaderParams = new HashMap<>();

    }

    @Parameters(commandNames = {"create-csv"}, commandDescription = "Create CSV filesImport the built data models in format CSV files into the database")
    public class CreateCsvCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input directory that contains the biological files to convert to CSV files)", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output directory where to save the CSV files to import", arity = 1)
        public String output;

        @Parameter(names = {"--clinical-analysis"}, description = "Input JSON files contains clinical analysis (otherwise, variants)", arity = 0)
        public boolean clinicalAnalysis = false;

        @Parameter(names = {"--exclude"}, description = "Exclude information separated by comma, e.g.:'XREF_DBNAME:Reactome Database ID Release 63'", arity = 1)
        public List<String> exclude;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        public Map<String, String> loaderParams = new HashMap<>();

    }

    @Parameters(commandNames = {"import"}, commandDescription = "Import the built data models in format CSV files into the database")
    public class ImportCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input directory where the CSV files are located", required = true, arity = 1)
        public String input;


        @Parameter(names = {"--database"}, description = "Data model type to be loaded, i.e. genome, gene, ...", arity = 1)
        public String database;

        @Parameter(names = {"--exclude"}, description = "Exclude information separated by comma, e.g.:'XREF_DBNAME:Reactome Database ID Release 63'", arity = 1)
        public List<String> exclude;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        public Map<String, String> loaderParams = new HashMap<>();

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


    @Parameters(commandNames = {"variant-annotation"}, commandDescription = "Annotate variants from VCF files using CellBase and other custom files")
    public class VariantAnnotationCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "Input file with the data file to be annotated", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output-file"}, description = "Output file with the annotations", required = false, arity = 1)
        public String output;

        @Parameter(names = {"--database"}, description = "Data model type to be loaded, i.e. genome, gene, ...", required = false, arity = 1)
        public String database;

        @Parameter(names = {"-s", "--species"}, description = "Name of the species to be downloaded, valid format include 'Homo sapiens' or 'hsapiens'", required = true, arity = 1)
        public String species = "Homo sapiens";

        @Parameter(names = {"-a", "--assembly"}, description = "Name of the assembly, if empty the first assembly in configuration.json will be read", required = false, arity = 1)
        public String assembly = "GRCh37";

        @Parameter(names = {"--remote-url"}, description = "The URL of CellBase REST web services, this has no effect if --local is present", required = false, arity = 1)
        public String url = "bioinfodev.hpc.cam.ac.uk";

        @Parameter(names = {"--remote-port"}, description = "The port where REST web services are listening", required = false, arity = 1)
        public int port = 80;

        @Parameter(names = {"-t", "--num-threads"}, description = "Number of threads to be used for loading", required = false, arity = 1)
        public int numThreads = 4;

        @Parameter(names = {"--batch-size"}, description = "Number of variants per batch", required = false, arity = 1)
        public int batchSize = 200;

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


    public void printUsage(){
        if(getCommand().isEmpty()) {
            System.err.println("");
            System.err.println("Program:     BioNetDB (OpenCB)");
            System.err.println("Version:     1.0.0");
            System.err.println("Description: BioNetDB implements a storage engine to work with biological networks using a NoQSL Graph database");
            System.err.println("");
            System.err.println("Usage:       bionetdb.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedCommand = getCommand();
            System.err.println("");
            System.err.println("Usage:   bionetdb.sh " + parsedCommand + " [options]");
            System.err.println("");
            System.err.println("OptionsFilter:");
            printCommandUsage(jcommander.getCommands().get(parsedCommand));
            System.err.println("");
        }
    }

    private void printMainUsage() {
        for (String s : jcommander.getCommands().keySet()) {
            System.err.printf("%20s  %s\n", s, jcommander.getCommandDescription(s));
        }
    }

    private void printCommandUsage(JCommander commander) {
        for (ParameterDescription parameterDescription : commander.getParameters()) {
            String type = "";
            if (parameterDescription.getParameterized().getParameter() != null && parameterDescription.getParameterized().getParameter().arity() > 0) {
                type = parameterDescription.getParameterized().getGenericType().getTypeName().replace("java.lang.", "").toUpperCase();
            }
            if(!parameterDescription.isHelp()) {
                System.err.printf("%5s %-20s %-10s %s [%s]\n",
                        (parameterDescription.getParameterized().getParameter() != null
                                && parameterDescription.getParameterized().getParameter().required()) ? "*": "",
                        parameterDescription.getNames(),
                        type,
                        parameterDescription.getDescription(),
                        parameterDescription.getDefault());
            } else {
                // This prints 'help' usage with no default [false] value
                System.err.printf("%5s %-20s %-10s %s\n",
                        (parameterDescription.getParameterized().getParameter() != null
                                && parameterDescription.getParameterized().getParameter().required()) ? "*": "",
                        parameterDescription.getNames(),
                        type,
                        parameterDescription.getDescription());
            }
        }
    }

    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public BuildCommandOptions getBuildCommandOptions() {
        return buildCommandOptions;
    }

    public LoadCommandOptions getLoadCommandOptions() {
        return loadCommandOptions;
    }

    public CreateCsvCommandOptions getCreateCsvCommandOptions() { return createCsvCommandOptions; }

    public ImportCommandOptions getImportCommandOptions() { return importCommandOptions; }

    public QueryCommandOptions getQueryCommandOptions() {
        return queryCommandOptions;
    }

    public ExpressionCommandOptions getExpressionCommandOptions () { return expressionCommandOptions; }

    public VariantAnnotationCommandOptions getVariantAnnotationCommandOptions() {
        return variantAnnotationCommandOptions;
    }

}
