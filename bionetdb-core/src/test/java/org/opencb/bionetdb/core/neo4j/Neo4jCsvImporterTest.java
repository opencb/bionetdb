package org.opencb.bionetdb.core.neo4j;

import org.junit.Test;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.utils.CsvInfo;
import org.opencb.bionetdb.core.utils.Neo4jCsvImporter;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class Neo4jCsvImporterTest {
    //-----------------------------------------
    // Clinical analysis
    //-----------------------------------------

    @Test
    public void createCsvFilesForClinicalAnalysys() throws BioNetDBException, URISyntaxException, IOException, InterruptedException {
        String caFilename = "/home/jtarraga/data150/clinicalAnalysis/input/clinicalAnalysis.json";
        String csvDirname= "/home/jtarraga/data150/clinicalAnalysis/csv";

        Path input = Paths.get(caFilename);

        Path output = Paths.get(csvDirname);
        output.toFile().delete();
        if (!output.toFile().exists()) {
            output.toFile().mkdirs();
        }

        // Prepare CSV object
        CsvInfo csv = new CsvInfo(input, output);

        // Open CSV files
        csv.openCSVFiles();

        Neo4jCsvImporter importer = new Neo4jCsvImporter(csv);

        List<File> files = new ArrayList<>();
        files.add(input.toFile());

        importer.addClinicalAnalysisFiles(files);

        // Close CSV files
        csv.close();
    }
}