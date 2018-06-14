package org.opencb.bionetdb.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.utils.CsvInfo;
import org.opencb.bionetdb.core.utils.Neo4jCsvImporter;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BioNetDbManagerTest {

    private String database = "scerevisiae";
    private BioNetDbManager bioNetDbManager;

    @Before
    public void initialize () {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig: bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }
            bioNetDbManager = new BioNetDbManager(database, bioNetDBConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (BioNetDBException e) {
            e.printStackTrace();
        }
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    @Test
    public void createCsvFiles() throws BioNetDBException, URISyntaxException, IOException, InterruptedException {
        Path input = Paths.get("~/data150/load.neo/illumina_platinum.export.5k.json");

        Path output = Paths.get("/tmp/csv");
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
        importer.addVariantFiles(files);

        // Close CSV files
        csv.close();
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    @Test
    public void getNodeByUid() throws Exception {
        QueryResult<Node> queryResult = bioNetDbManager.getNode(1);
        printNodes(queryResult.getResult());
    }

    @Test
    public void nodeQuery() throws Exception {
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "GENE,TRANSCRIPT");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        QueryResult<Node> queryResult = bioNetDbManager.nodeQuery(cypher.toString());
        printNodes(queryResult.getResult());
    }

    @Test
    public void nodeQueryByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match (g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return g,t");
        QueryResult<Node> queryResult = bioNetDbManager.nodeQuery(cypher.toString());
        printNodes(queryResult.getResult());
    }

    @Test
    public void tableQuery() throws Exception {
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "GENE.name,GENE.id,TRANSCRIPT.id");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        QueryResult<List<Object>> queryResult = bioNetDbManager.table(cypher);
        printLists(queryResult.getResult());
    }

    @Test
    public void tableByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match (g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return g.uid, g.id, t.uid, t.id, g");
        QueryResult<List<Object>> queryResult = bioNetDbManager.table(cypher.toString());
        printLists(queryResult.getResult());
    }

    @Test
    public void networkQuery() throws Exception {
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
        query.put(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key(), "TRANSCRIPT");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "network");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        QueryResult<Network> queryResult = bioNetDbManager.networkQuery(cypher);
        System.out.println(queryResult.getResult().get(0).toString());
    }

    @Test
    public void networkByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match path=(g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return path");
        QueryResult<Network> queryResult = bioNetDbManager.networkQuery(cypher.toString());
        System.out.println(queryResult.getResult().get(0).toString());
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    private void printNodes(List<Node> nodes) {
        for (Node node: nodes) {
            System.out.println(node.toStringEx());
        }
    }

    private void printLists(List<List<Object>> lists) {
        for (List<Object> list: lists) {
            for (Object item: list) {
                System.out.print(item + ", ");
            }
        }
    }

    @Test
    public void getGene() throws IOException {
        String geneId = "ENSG00000164053";
        String transcriptId = "ENST00000424906";

        // CellBase client
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", "GRCh37", clientConfiguration);
        GeneClient geneClient = cellBaseClient.getGeneClient();

        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,transcripts.cDnaSequence,annotation.expression");
        List<String> ids = new ArrayList<>();
        ids.add(geneId);
        QueryResponse<Gene> geneQueryResponse = geneClient.get(ids, options);
        for (QueryResult<Gene> result: geneQueryResponse.getResponse()) {
            for (Gene gene: result.getResult()) {
                System.out.println(gene.getId() + ", " + gene.getName());
                for (Transcript transcript: gene.getTranscripts()) {
                    System.out.println("\t" + transcript.getId() + ", " + transcript.getName());
                    if (transcript.getId().equals(transcriptId)) {
                        System.out.println("\t\tFOUND !!!!");
                    }
                }
            }
        }
    }

    @Test
    public void getGenes() throws IOException {
        String assembly = "GRCh38"; // "GRCh37", "GRCh38"
        // CellBase client
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", assembly, clientConfiguration);

        GeneClient geneClient = cellBaseClient.getGeneClient();
        Query query = new Query();
        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,transcripts.cDnaSequence,annotation.expression");
        QueryResponse<Long> countResponse = geneClient.count(query);
        long numGenes = countResponse.firstResult();
        int bufferSize = 400;
        options.put(QueryOptions.LIMIT, bufferSize);
        System.out.println("Num. genes: " + numGenes);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter writer = mapper.writer();
        PrintWriter pw = new PrintWriter(Paths.get("/tmp/" + assembly + ".genes.json").toString());
        for (int i = 0; i < numGenes; i+=bufferSize) {
            options.put(QueryOptions.SKIP, i);
            QueryResponse<Gene> geneResponse = geneClient.search(query, options);
            for (Gene gene: geneResponse.allResults()) {
                String json = writer.writeValueAsString(gene);
                pw.println(json);
            }
            System.out.println("Processing " + i + " of " + numGenes);
        }
        pw.close();
    }

    @Test
    public void getProteins() throws IOException {
        String assembly = "GRCh38"; // "GRCh37", "GRCh38"
        // CellBase client
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", assembly, clientConfiguration);

        ProteinClient proteinClient = cellBaseClient.getProteinClient();
        Query query = new Query();
        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "reference,comment,sequence,evidence");
        long numProteins = 100000;
        int bufferSize = 400;
        options.put(QueryOptions.LIMIT, bufferSize);
        System.out.println("Num. proteins: " + numProteins);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter writer = mapper.writer();
        PrintWriter pw = new PrintWriter(Paths.get("/tmp/" + assembly + ".proteins.json").toString());
        for (int i = 0; i < numProteins; i+=bufferSize) {
            options.put(QueryOptions.SKIP, i);
            QueryResponse<Entry> proteinResponse = proteinClient.search(query, options);
            if (proteinResponse.allResults().size() == 0) {
                break;
            }
            for (Entry entry: proteinResponse.allResults()) {
                String json = writer.writeValueAsString(entry);
                pw.println(json);
            }
            System.out.println("Processing " + i + " of " + numProteins);
        }
        pw.close();
    }
}