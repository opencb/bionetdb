package org.opencb.bionetdb.core.neo4j;

public class Neo4jCsvImporterTest {
    //-----------------------------------------
    // Clinical analysis
    //-----------------------------------------

//    @Test
//    public void createCsvFilesForClinicalAnalysys() throws BioNetDBException, URISyntaxException, IOException, InterruptedException {
//        String caFilename = "/home/jtarraga/data150/clinicalAnalysis/input/clinicalAnalysis.json";
//        String csvDirname= "/home/jtarraga/data150/clinicalAnalysis/csv";
//
//        Path input = Paths.get(caFilename);
//
//        Path output = Paths.get(csvDirname);
//        output.toFile().delete();
//        if (!output.toFile().exists()) {
//            output.toFile().mkdirs();
//        }
//
//        // Prepare CSV object
//        CsvInfo csv = new CsvInfo(input, output);
//
//        // Open CSV files
//        csv.openCSVFiles();
//
//        Neo4jCsvImporter importer = new Neo4jCsvImporter(csv);
//
//        List<File> files = new ArrayList<>();
//        files.add(input.toFile());
//
//        importer.addClinicalAnalysisFiles(files);
//
//        // Close CSV files
//        csv.close();
//    }
}