package org.opencb.bionetdb.lib.utils;

import org.apache.commons.lang.StringEscapeUtils;
import org.opencb.bionetdb.lib.db.Neo4jBioPaxBuilder;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Importer {

    private String database;
    private Path inputPath;

    protected static Logger logger;

    public Importer() {
        this.logger = LoggerFactory.getLogger(this.getClass().toString());
    }

    public Importer(String database, Path inputPath) {
        this.database = database;
        this.inputPath = inputPath;
        this.logger = LoggerFactory.getLogger(this.getClass().toString());
    }

    public void run() {
        // Check input directory
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
        sb.append("/bin/neo4j-admin import --database " + database + " --id-type INTEGER --delimiter=\""
                + StringEscapeUtils.escapeJava(CsvInfo.SEPARATOR) + "\" " + "--skip-duplicate-nodes");

        // Retrieving files from the input directory
        List<File> relationFiles = new ArrayList<>();
        for (File file : inputPath.toFile().listFiles()) {
            if (file.isFile()) {
                String filename = file.getName();
                if (filename.endsWith(".csv")) {
                    if (filename.contains("__")) {
                        relationFiles.add(file);
                    } else {
                        if (!isEmptyCsvFile(file)) {
                            name = getNodeName(file);
                            validNodes.add(name);
                            sb.append(" --nodes=").append(name).append("=").append(file.getAbsolutePath());
                        } else {
                            logger.info("Skipping node file {} to import: file empty", file.getAbsolutePath());
                        }
                    }
                } else {
                    logger.info("Skipping node file {} to import: no CSV format", file.getAbsolutePath());
                }
            }
        }

        // Now, relationhsip files
        for (File file : relationFiles) {
            System.out.println("checking file: " + file.getName());
            if (isValidRelationCsvFile(file, validNodes)) {
                name = getRelationName(file);
                sb.append(" --relationships=").append(name).append("=").append(file.getAbsolutePath());
            } else {
                logger.info("Skipping relationship file {} to import: invalid file", file.getAbsolutePath());
            }
        }

        System.out.println(sb.toString());

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
            if (name.contains(Neo4jBioPaxBuilder.BioPAXProcessing.SEPARATOR)) {
                String[] fields = removeCsvExt(name).split(Neo4jBioPaxBuilder.BioPAXProcessing.SEPARATOR);
                isValid = nodeNames.contains(fields[1]) && nodeNames.contains(fields[2]);
            } else {
                String[] fields = removeCsvExt(name).split("__");
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
        if (name.contains(Neo4jBioPaxBuilder.BioPAXProcessing.SEPARATOR)) {
            return name.split(Neo4jBioPaxBuilder.BioPAXProcessing.SEPARATOR)[0];
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
}
