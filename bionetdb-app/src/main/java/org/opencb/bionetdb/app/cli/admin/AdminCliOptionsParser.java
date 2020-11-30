package org.opencb.bionetdb.app.cli.admin;

import com.beust.jcommander.*;
import org.opencb.bionetdb.app.cli.CliOptionsParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 19/10/20.
 */
public class AdminCliOptionsParser extends CliOptionsParser {

//    private final JCommander jcommander;
//
//    private final GeneralOptions generalOptions;
    private final CommonCommandOptions commonCommandOptions;

    private DownloadCommandOptions downloadCommandOptions;
    private BuildCommandOptions buildCommandOptions;
    private ImportCommandOptions importCommandOptions;
    private LoadCommandOptions loadCommandOptions;


    public AdminCliOptionsParser() {
//        generalOptions = new GeneralOptions();

//        jcommander = new JCommander(generalOptions);
        programName = "bionetdb-admin.sh";
        jCommander.setProgramName(programName);

        commonCommandOptions = new CommonCommandOptions();

        downloadCommandOptions = new DownloadCommandOptions();
        buildCommandOptions = new BuildCommandOptions();
        importCommandOptions = new ImportCommandOptions();
        loadCommandOptions = new LoadCommandOptions();

        jCommander.addCommand("download", downloadCommandOptions);
        jCommander.addCommand("build", buildCommandOptions);
        jCommander.addCommand("import", importCommandOptions);
        jCommander.addCommand("load", loadCommandOptions);
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
            JCommander jc = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jc.getObjects();
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

        @Parameter(names = {"-C", "--conf"}, description = "BioNetDB configuration file (JSON format).", required = false, arity = 1)
        public String conf;

    }

    @Parameters(commandNames = {"download"}, commandDescription = "Download all different data sources provided in the configuration.yml file")
    public class DownloadCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-o", "--outdir"}, description = "Downloaded files will be saved in this directory.", required = true, arity = 1)
        public String outDir;
    }


    @Parameters(commandNames = {"build"}, commandDescription = "Build the data models in CSV format files")
    public class BuildCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input directory that contains the biological files to convert to CSV files)", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output directory where to save the CSV files to import", required = true, arity = 1)
        public String output;

        @Parameter(names = {"--add-network-file"}, description = "JSON file containing a BioNetDB network", arity = 1)
        public List<String> networkFiles;

        @Parameter(names = {"--exclude"}, description = "Exclude information separated by comma, e.g.:'XREF_DBNAME:Reactome Database ID Release 63'", arity = 1)
        public List<String> exclude;
    }

    @Parameters(commandNames = {"import"}, commandDescription = "Import the built data models in format CSV files into the BioNetDB database")
    public class ImportCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input directory where the CSV files are located", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-d", "--database"}, description = "BioNetDB database name", required = true, arity = 1)
        public String database;
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

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public DownloadCommandOptions getDownloadCommandOptions() {
        return downloadCommandOptions;
    }

    public BuildCommandOptions getBuildCommandOptions() {
        return buildCommandOptions;
    }

    public ImportCommandOptions getImportCommandOptions() { return importCommandOptions; }

    public LoadCommandOptions getLoadCommandOptions() {
        return loadCommandOptions;
    }
}
