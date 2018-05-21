package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.CSVInfo;
import org.opencb.bionetdb.core.utils.Neo4JBioPAXImporter;
import org.opencb.bionetdb.core.utils.Neo4JCSVImporter;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.SingleProcess;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jtarraga on 21/06/18.
 */
public class ImportCommandExecutor extends CommandExecutor {

    private CliOptionsParser.ImportCommandOptions importCommandOptions;

    public ImportCommandExecutor(CliOptionsParser.ImportCommandOptions importCommandOptions) {
        super(importCommandOptions.commonOptions.logLevel, importCommandOptions.commonOptions.conf);

        this.importCommandOptions = importCommandOptions;
    }

    @Override
    public void execute() {
        if (importCommandOptions.createCsvFiles) {
            createCsvFiles();
        } else {
            importCsvFiles();
        }
    }

    private void createCsvFiles() {
        try {
            // Check input and output directories
            Path inputPath = Paths.get(importCommandOptions.input);
            if (!inputPath.toFile().isDirectory()) {
                logger.error("Input directory {} is invalid", inputPath);
                return;
            }

            Path outputPath = Paths.get(importCommandOptions.output);
            if (!outputPath.toFile().isDirectory()) {
                logger.error("Output directory {} is invalid", inputPath);
                return;
            }

            // Prepare CSV object
            CSVInfo csv = new CSVInfo(outputPath);

            // Open CSV files
            csv.openCSVFiles();
            Neo4JCSVImporter importer = new Neo4JCSVImporter(csv);

            // CellBase client
            // TODO: maybe it should be created from the configuration file
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setVersion("v4");
            clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
            CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", "GRCh38", clientConfiguration);

            // Retrieving files from the input directory
            List<File> reactomeFiles = new ArrayList<>();
            List<File> jsonFiles = new ArrayList<>();
            File firstFile = null;
            long minSize = Long.MAX_VALUE;
            for (File file: inputPath.toFile().listFiles()) {
                if (file.isFile()) {
                    String filename = file.getName();
                    if (filename.contains("biopax") || filename.contains("owl")) {
                        reactomeFiles.add(file);
                    } else if (filename.endsWith(".json") || filename.endsWith(".json.gz")) {
                        jsonFiles.add(file);
                        if (file.length() < minSize) {
                            minSize = file.length();
                            firstFile = file;
                        }
                    } else {
                        logger.info("Skipping file {}", file.getAbsolutePath());
                    }
                }
            }

            if (firstFile != null) {
                // The first variant file to process is the smaller one
                jsonFiles.remove(firstFile);
                jsonFiles.add(0, firstFile);
                logger.info("Setting first JSON file to process: {}", firstFile.getAbsolutePath());
            }

            // Import BioPAX files
            Map<String, Set<String>> filters = parseFilters(importCommandOptions.exclude);
            Neo4JBioPAXImporter bioPAXImporter = new Neo4JBioPAXImporter(csv, filters, new BPAXProcessing(csv));
            bioPAXImporter.addReactomeFiles(reactomeFiles);

            // Annotate proteins
            importer.annotateProteins(cellBaseClient);

            // JSON variant files
            importer.addVariantFiles(jsonFiles);

            // Annotate genes
            importer.annotateGenes(cellBaseClient);

            // Close CSV files
            csv.close();
        } catch (IOException e) {
            logger.error("Error generation CSV files: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void importCsvFiles() {
        // Check input directories
        Path inputPath = Paths.get(importCommandOptions.input);
        if (!inputPath.toFile().isDirectory()) {
            logger.error("Input directory {} is invalid", inputPath);
            return;
        }

        String name;
        StringBuilder sb = new StringBuilder();
        Set<String> validNodes = new HashSet<>();

        String neo4jHome = System.getenv("NEO4J_HOME");
        if (neo4jHome == null) {
            logger.error("The environment variable NEO4J_HOME is not defined.");
            return;
        }
        sb.append(neo4jHome);
        sb.append("/bin/neo4j stop");
        Command command = new Command(sb.toString());
        command.run();
        logger.info("Executing: {}", sb);
        logger.info("Output: {}", command.getOutput());

        sb.setLength(0);
        sb.append(neo4jHome);
        sb.append("/bin/neo4j-admin import --ignore-duplicate-nodes --ignore-missing-nodes");

        // Retrieving files from the input directory
        List<File> relationFiles = new ArrayList<>();
        for (File file: inputPath.toFile().listFiles()) {
            if (file.isFile()) {
                String filename = file.getName();
                if (filename.endsWith(".csv")) {
                    if (filename.contains("__")) {
                        relationFiles.add(file);
                    } else {
                        if (!isEmptyCsvFile(file)) {
                            name = getNodeName(file);
                            validNodes.add(name);
                            sb.append(" --nodes:").append(name).append(" ").append(file.getAbsolutePath());
                        }
                    }
                } else {
                    logger.info("Skipping file {} to import", file.getAbsolutePath());
                }
            }
        }

        // Now, relationhsip files
        for (File file: relationFiles) {
            if (isValidRelationCsvFile(file, validNodes)) {
                name = getRelationName(file);
                sb.append(" --relationships:").append(name).append(" ").append(file.getAbsolutePath());
            }
        }

        command = new Command(sb.toString());
        command.run();
        logger.info("Executing: {}", sb);
        logger.info("Output: {}", command.getOutput());

        sb.setLength(0);
        sb.append(neo4jHome);
        sb.append("/bin/neo4j start");
        command = new Command(sb.toString());
        command.run();
        logger.info("Executing: {}", sb);
        logger.info("Output: {}", command.getOutput());
    }

    private boolean isEmptyCsvFile(File file) {
        boolean isEmpty = false;
        try {
            BufferedReader bufferedReader = FileUtils.newBufferedReader(file.toPath());
            int counter = 0;
            String line = bufferedReader.readLine();
            while (line != null) {
                counter++;
                if (counter > 1) {
                    break;
                }
                line = bufferedReader.readLine();
            }
            isEmpty = (counter < 2);
        } catch (IOException e) {
            logger.warn("Something wrong checking CSV file {}, skipping it!", file.toPath());
            isEmpty = false;
        }
        return isEmpty;
    }

    private boolean isValidRelationCsvFile(File file, Set<String> nodeNames) {
        boolean isValid = false;
        if (!isEmptyCsvFile(file)) {
            String name = file.getName();
            if (name.contains(Neo4JBioPAXImporter.BioPAXProcessing.SEPARATOR)) {
                name = name.split(Neo4JBioPAXImporter.BioPAXProcessing.SEPARATOR)[0];
                isValid = nodeNames.contains(name);
            } else {
                String fields[] = removeCsvExt(name).split("__");
                isValid = nodeNames.contains(fields[0]) && nodeNames.contains(fields[1]);
            }
        }
        return isValid;
    }

    private String getNodeName(File file) {
        return removeCsvExt(file.getName());
    }

    private String getRelationName(File file) {
        String name = file.getName();
        if (name.contains(Neo4JBioPAXImporter.BioPAXProcessing.SEPARATOR)) {
            return name.split(Neo4JBioPAXImporter.BioPAXProcessing.SEPARATOR)[0];
        } else {
            return removeCsvExt(name);
        }
    }

    private String removeCsvExt(String filename) {
        String name = null;
        if (filename != null) {
            return filename.replace(".csv", "");
        }
        return name;
    }

    //-------------------------------------------------------------------------
    //  BioPAX importer callback object
    //-------------------------------------------------------------------------

    public class BPAXProcessing implements Neo4JBioPAXImporter.BioPAXProcessing {
        private CSVInfo csv;

        public BPAXProcessing(CSVInfo csv) {
            this.csv = csv;
        }

        @Override
        public void processNodes(List<Node> nodes) {
            PrintWriter pw;
            for (Node node: nodes) {
                pw = csv.getCsvWriters().get(node.getType().toString());
                pw.println(csv.nodeLine(node));
            }
        }

        // Debug purposes
        private Set<String> notdefined = new HashSet<>();

        @Override
        public void processRelations(List<Relation> relations) {
            PrintWriter pw;
            for (Relation relation: relations) {
                String id = relation.getType()
                        + Neo4JBioPAXImporter.BioPAXProcessing.SEPARATOR + relation.getOrigType()
                        + Neo4JBioPAXImporter.BioPAXProcessing.SEPARATOR + relation.getDestType();
                pw = csv.getCsvWriters().get(id);
                if (pw == null) {
                    if (!notdefined.contains(id)) {
                        logger.info("BioPAX relationship not yet supported {}", id);
                        notdefined.add(id);
                    }
                } else {
                    pw.println(csv.relationLine(relation.getOrigUid(), relation.getDestUid()));
                }
            }
        }
    }
    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

//
//        try {
//            if (configuration == null || configuration.getDatabases() == null || configuration.getDatabases().size() == 0) {
//                logger.error("Configuration objects is null or databases are empty");
//                return;
//            }
//
//            Path inputPath = Paths.get(importCommandOptions.input);
//            FileUtils.checkFile(inputPath);
//
//            // BioNetDBManager checks if database parameter is empty
//            BioNetDBManager bioNetDBManager = new BioNetDBManager(importCommandOptions.database, configuration);
//
//
//
//
//
//            //bioNetDBManager.loadBioPax(inputPath, filter);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public Map<String, Set<String>> parseFilters(List<String> excludeList) {
        Map<String, Set<String>> filters = null;
        if (ListUtils.isNotEmpty(excludeList)) {
            filters = new HashMap<>();
            for (String exclude: excludeList) {
                String split[] = exclude.split(":");
                if (split.length == 2) {
                    if (!filters.containsKey(split[0])) {
                        filters.put(split[0], new HashSet<>());
                    }
                    filters.get(split[0]).add(split[1]);
                }
            }
        }
        return filters;
    }

}
