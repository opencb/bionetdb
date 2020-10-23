package org.opencb.bionetdb.lib.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.neo4j.driver.Values.parameters;

public class Neo4JLoaderTest {
    private Driver driver;
    private Session session;

    @Before
    public void init() {
        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "neo4j;";
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> driver.close()));
        session = driver.session();
        System.out.println("Init. Done!");
    }

    @After
    public void clean() {
        session.close();
        System.out.println("Clean. Done!");
    }

    @Test
    public void testClinicalAnalysis() throws IOException {
        long counter = 0;
        // Reading file line by line, each line a
        // JSON object
        Path path = Paths.get("/home/jtarraga/data150/clinicalAnalysis/input/clinicalAnalysis.json");
        BufferedReader reader = FileUtils.newBufferedReader(path);

        String line = reader.readLine();
        while (line != null) {
            counter++;
            System.out.println("line " + counter + ", length = " + line.length());

            // Call user defined procedure: loadClinicalAnalysis
            session.run( "CALL org.opencb.bionetdb.core.neo4j.loadClinicalAnalysis($caJson)", parameters( "caJson", line));

            // Read next line
            line = reader.readLine();
        }
        System.out.println("Clinical analysis: " + counter);

        reader.close();
    }

//    @Test
//    public void getGenes() throws IOException {
//        String assembly = "GRCh38"; // "GRCh37", "GRCh38"
//        // CellBase client
//        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        clientConfiguration.setVersion("v4");
//        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
//        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", assembly, clientConfiguration);
//
//        GeneClient geneClient = cellBaseClient.getGeneClient();
//        Query query = new Query();
//        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,transcripts.cDnaSequence,annotation.expression");
//        CellBaseDataResponse<Long> countResponse = geneClient.count(query);
//        long numGenes = countResponse.firstResult();
//        int bufferSize = 400;
//        options.put(QueryOptions.LIMIT, bufferSize);
//        System.out.println("Num. genes: " + numGenes);
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
//        ObjectWriter writer = mapper.writer();
//        PrintWriter pw = new PrintWriter(Paths.get("/tmp/" + assembly + ".genes.json").toString());
//        for (int i = 0; i < numGenes; i += bufferSize) {
//            options.put(QueryOptions.SKIP, i);
//            CellBaseDataResponse<Gene> geneResponse = geneClient.search(query, options);
//            for (Gene gene : geneResponse.allResults()) {
//                String json = writer.writeValueAsString(gene);
//                pw.println(json);
//            }
//            System.out.println("Processing " + i + " of " + numGenes);
//        }
//        pw.close();
//    }
//
//    @Test
//    public void getProteins() throws IOException {
//        String assembly = "GRCh38"; // "GRCh37", "GRCh38"
//        // CellBase client
//        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        clientConfiguration.setVersion("v4");
//        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
//        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", assembly, clientConfiguration);
//
//        ProteinClient proteinClient = cellBaseClient.getProteinClient();
//        Query query = new Query();
//        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "reference,comment,sequence,evidence");
//        long numProteins = 100000;
//        int bufferSize = 400;
//        options.put(QueryOptions.LIMIT, bufferSize);
//        System.out.println("Num. proteins: " + numProteins);
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
//        ObjectWriter writer = mapper.writer();
//        PrintWriter pw = new PrintWriter(Paths.get("/tmp/" + assembly + ".proteins.json").toString());
//        for (int i = 0; i < numProteins; i += bufferSize) {
//            options.put(QueryOptions.SKIP, i);
//            CellBaseDataResponse<Entry> proteinResponse = proteinClient.search(query, options);
//            if (proteinResponse.allResults().size() == 0) {
//                break;
//            }
//            for (Entry entry : proteinResponse.allResults()) {
//                String json = writer.writeValueAsString(entry);
//                pw.println(json);
//            }
//            System.out.println("Processing " + i + " of " + numProteins);
//        }
//        pw.close();
//    }
}