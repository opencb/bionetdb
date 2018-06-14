package org.opencb.bionetdb.app.cli;

import org.apache.commons.lang.StringUtils;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.CsvInfo;
import org.opencb.bionetdb.core.utils.Neo4jBioPaxImporter;
import org.opencb.bionetdb.core.utils.Neo4jCsvImporter;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.io.BufferedReader;
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
            long start;

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
            CsvInfo csv = new CsvInfo(inputPath, outputPath);

            // Open CSV files
            csv.openCSVFiles();
            Neo4jCsvImporter importer = new Neo4jCsvImporter(csv);

            // CellBase client
            // TODO: maybe it should be created from the configuration file
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setVersion("v4");
            clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
            CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", "GRCh38", clientConfiguration);

            // Retrieving files from the input directory
            List<File> reactomeFiles = new ArrayList<>();
            List<File> jsonFiles = new ArrayList<>();
            long minSize = Long.MAX_VALUE;
            for (File file: inputPath.toFile().listFiles()) {
                if (file.isFile()) {
                    String filename = file.getName();
                    if (filename.equals(Neo4jCsvImporter.GENE_FILENAME)
                            || filename.equals(Neo4jCsvImporter.PROTEIN_FILENAME)) {
                        continue;
                    }
                    if (filename.contains("biopax") || filename.contains("owl")) {
                        reactomeFiles.add(file);
                    } else if (filename.endsWith("meta.json")) {
                        continue;
                    } else if (filename.endsWith(".json") || filename.endsWith(".json.gz")) {
                        jsonFiles.add(file);
                    } else {
                        logger.info("Skipping file {}", file.getAbsolutePath());
                    }
                }
            }

            long geneIndexingTime, proteinIndexingTime, miRnaIndexingTime, bioPaxTime;

            // Indexing genes
            logger.info("Starting gene indexing...");
            start = System.currentTimeMillis();
            importer.indexingGenes(Paths.get(inputPath + "/" + Neo4jCsvImporter.GENE_FILENAME),
                    Paths.get(outputPath + "/" + Neo4jCsvImporter.GENE_DBNAME));
            geneIndexingTime = (System.currentTimeMillis() - start) / 1000;
            logger.info("Gene indexing done in {} s", geneIndexingTime);

            // Indexing proteins
            logger.info("Starting protein indexing...");
            start = System.currentTimeMillis();
            importer.indexingProteins(Paths.get(inputPath + "/" + Neo4jCsvImporter.PROTEIN_FILENAME),
                    Paths.get(outputPath + "/" + Neo4jCsvImporter.PROTEIN_DBNAME));
            proteinIndexingTime = (System.currentTimeMillis() - start) / 1000;
            logger.info("Protein indexing done in {} s", proteinIndexingTime);

            // Indexing miRNA
            logger.info("Starting miRNA indexing...");
            start = System.currentTimeMillis();
            importer.indexingMiRnas(Paths.get(inputPath + "/" + Neo4jCsvImporter.MIRNA_FILENAME),
                    Paths.get(outputPath + "/" + Neo4jCsvImporter.MIRNA_DBNAME));
            miRnaIndexingTime = (System.currentTimeMillis() - start) / 1000;
            logger.info("miRNA indexing done in {} s", miRnaIndexingTime);

            // Parse BioPAX files
            Map<String, Set<String>> filters = parseFilters(importCommandOptions.exclude);
            BPAXProcessing bpaxProcessing = new BPAXProcessing(importer);
            Neo4jBioPaxImporter bioPAXImporter = new Neo4jBioPaxImporter(csv, filters, bpaxProcessing);
            start = System.currentTimeMillis();
            bioPAXImporter.addReactomeFiles(reactomeFiles);
            bpaxProcessing.post();
            bioPaxTime = (System.currentTimeMillis() - start) / 1000;


            // Parse JSON variant files
            start =  System.currentTimeMillis();
            importer.addVariantFiles(jsonFiles);
            long variantTime = (System.currentTimeMillis() - start) / 1000;

            // Close CSV files
            csv.close();

            logger.info("Gene indexing in {} s", geneIndexingTime);
            logger.info("Protein indexing in {} s", proteinIndexingTime);
            logger.info("miRNA indexing in {} s", miRnaIndexingTime);
            logger.info("BioPAX processing in {} s", bioPaxTime);
            logger.info("Variant processing in {} s", variantTime);
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
            System.out.println("checking file: " + file.getName());
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
            if (name.contains(Neo4jBioPaxImporter.BioPAXProcessing.SEPARATOR)) {
                String fields[] = removeCsvExt(name).split(Neo4jBioPaxImporter.BioPAXProcessing.SEPARATOR);
                isValid = nodeNames.contains(fields[1]) && nodeNames.contains(fields[2]);
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
        if (name.contains(Neo4jBioPaxImporter.BioPAXProcessing.SEPARATOR)) {
            return name.split(Neo4jBioPaxImporter.BioPAXProcessing.SEPARATOR)[0];
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

    public class BPAXProcessing implements Neo4jBioPaxImporter.BioPAXProcessing {
        private Neo4jCsvImporter importer;

        private List<Node> dnaNodes;
        private List<Node> rnaNodes;


        public BPAXProcessing(Neo4jCsvImporter importer) {
            this.importer = importer;
            dnaNodes = new ArrayList<>();
            rnaNodes = new ArrayList<>();
        }

        public void post() {
            CsvInfo csv = importer.getCsvInfo();
            PrintWriter pwNode, pwRel;

            // Post-process gene nodes
            logger.info("Post-processing {} dna nodes", dnaNodes.size());
            pwNode = csv.getCsvWriters().get(Node.Type.DNA.toString());
            pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.IS___DNA___GENE.toString());
            for (Node node: dnaNodes) {
                List<String> geneIds = getGeneIds(node.getName());
                for (String geneId: geneIds) {
                    Long geneUid = importer.processGene(geneId, geneId);
                    if (geneUid != null) {
                        // Write dna-gene relation
                        pwRel.println(node.getUid() + "," + geneUid);
                    }
                }
                // Write DNA node
                pwNode.println(csv.nodeLine(node));
            }

            // Post-process miRNA nodes
            logger.info("Post-processing {} miRNA nodes", rnaNodes.size());
            pwNode = csv.getCsvWriters().get(Node.Type.RNA.toString());
            PrintWriter pwMiRna = csv.getCsvWriters().get(Node.Type.MIRNA.toString());
            PrintWriter pwMiRnaTargetRel = csv.getCsvWriters().get(
                    CsvInfo.BioPAXRelation.TARGET_GENE___MIRNA___GENE.toString());
            pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.IS___RNA___MIRNA.toString());
            for (Node node: rnaNodes) {
                List<String> miRnaInfo = getMiRnaInfo(node.getName());
                for (String info: miRnaInfo) {
                    String[] fields = info.split(":");
                    String miRnaId = fields[0];
                    String targetGene = fields[1];
                    String evidence = fields[2];

                    Long miRnaUid = csv.getLong(miRnaId);
                    if (miRnaUid == null) {
                        Node miRnaNode = new Node(csv.getAndIncUid(), miRnaId, miRnaId, Node.Type.MIRNA);
                        pwMiRna.println(csv.nodeLine(miRnaNode));

                        // Save the miRNA node uid
                        miRnaUid = miRnaNode.getUid();
                        csv.putLong(miRnaId, miRnaUid);
                    }

                    // Process target gene
                    Long geneUid = importer.processGene(targetGene, targetGene);
                    if (geneUid != null) {
                        if (csv.getLong(miRnaUid + "." + geneUid) == null) {
                            // Write mirna-target gene relation
                            pwMiRnaTargetRel.println(miRnaUid + "," + geneUid + "," + evidence);
                            csv.putLong(miRnaUid + "." + geneUid, 1);
                        }
                    }

                    // Write rna-mirna relation
                    pwRel.println(node.getUid() + "," + miRnaUid);
                }
                // Write RNA node
                pwNode.println(importer.getCsvInfo().nodeLine(node));
            }
        }

        @Override
        public void processNodes(List<Node> nodes) {
            PrintWriter pw;
            for (Node node: nodes) {
                pw = importer.getCsvInfo().getCsvWriters().get(node.getType().toString());

                if (StringUtils.isNotEmpty(node.getName())) {
                    if (node.getType() == Node.Type.PROTEIN) {
                        // Complete node proteins
                        node = importer.completeProteinNode(node);
                    } else if (node.getType() == Node.Type.DNA) {
                        // Save save gene nodes to process further, in the post-processing phase
                        dnaNodes.add(node);
                        continue;
                    } else if (node.getType() == Node.Type.RNA) {
                        // Save save miRNA nodes to process further, in the post-processing phase
                        rnaNodes.add(node);
                        continue;
                    }
                }

                // Write node to CSV file
                pw.println(importer.getCsvInfo().nodeLine(node));
            }
        }

        @Override
        public void processRelations(List<Relation> relations) {
            PrintWriter pw;
            for (Relation relation: relations) {
                String id = relation.getType()
                        + Neo4jBioPaxImporter.BioPAXProcessing.SEPARATOR + relation.getOrigType()
                        + Neo4jBioPaxImporter.BioPAXProcessing.SEPARATOR + relation.getDestType();
                pw = importer.getCsvInfo().getCsvWriters().get(id);
                if (pw == null) {
                    logger.info("BioPAX relationship not yet supported {}", id);
                } else {
                    // Write relation to CSV file
                    pw.println(importer.getCsvInfo().relationLine(relation.getOrigUid(), relation.getDestUid()));
                }
            }
        }

        private List<String> getGeneIds(String nodeName) {
            List<String> ids = new ArrayList<>();
            if (nodeName.toLowerCase().contains("genes") || nodeName.contains("(") || nodeName.contains(",")
                    || nodeName.contains(";")) {
                return ids;
            } else {
                String name = nodeName.replace("gene", "").replace("Gene", "").trim();
                ids.add(name);
                return ids;
            }
        }

        private List<String> getMiRnaInfo(String nodeName) {
            String[] sufixes0 = {"", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
            String[] sufixes1 = {"-3p", "-5p"};
            List<String> list = new ArrayList<>();
            if (nodeName.contains("miR")) {
                for (String s0: sufixes0) {
                    for (String s1: sufixes1) {
                        String id = "hsa-" + nodeName + s0 + s1;
                        String info = importer.getCsvInfo().getMiRnaInfo(id);
                        if (info != null) {
                            list.add(info);
                        }
                    }
                }
            }
            return list;
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
//            // BioNetDbManager checks if database parameter is empty
//            BioNetDbManager bioNetDBManager = new BioNetDbManager(importCommandOptions.database, configuration);
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
