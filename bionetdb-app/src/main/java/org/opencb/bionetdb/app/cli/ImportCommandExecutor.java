package org.opencb.bionetdb.app.cli;

import org.opencb.bionetdb.core.BioNetDBManager;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.CSVInfo;
import org.opencb.bionetdb.core.utils.Neo4JBioPAXImporter;
import org.opencb.bionetdb.core.utils.Neo4JCSVImporter;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
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
        logger.info("Importing CSV files is not implemented yet!");
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
