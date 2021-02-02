package org.opencb.bionetdb.lib;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.lib.analysis.VariantAnalysis;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.db.query.Neo4JQueryParser;
import org.opencb.bionetdb.lib.utils.CsvInfo;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class BioNetDbManagerTest {

    private String database = "scerevisiae";
    private BioNetDbManager bioNetDbManager;


    @Before
    public void setUp() throws Exception {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            System.out.println(bioNetDBConfiguration.getDatabase());

            //bioNetDBConfiguration.getDatabases().get(0).setPort(6660);
            //bioNetDBConfiguration.getDatabase().setPort(27687);
            bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (bioNetDbManager != null) {
            bioNetDbManager.close();
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
//        csv.openCSVFiles(clinicalVariantFile.toString(), additionalVariantFiles);

//        Neo4jCsvImporter importer = new Neo4jCsvImporter(csv);
//
//        List<File> files = new ArrayList<>();
//        files.add(input.toFile());
//        importer.addVariantFiles(files);

        // Close CSV files
        csv.close();
    }

    //-------------------------------------------------------------------------
    //
    //-------------------------------------------------------------------------

    @Test
    public void getNodeByUid() throws Exception {
        DataResult<Node> queryResult = bioNetDbManager.getNodeQueryExecutor().getNode(1);
        printNodes(queryResult.getResults());
    }

    @Test
    public void nodeQuery() throws Exception {
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "GENE,TRANSCRIPT");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
        DataResult<Node> queryResult = bioNetDbManager.getNodeQueryExecutor().query(cypher.toString());
        printNodes(queryResult.getResults());
    }

    @Test
    public void nodeQueryByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match (g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return g,t");
        DataResult<Node> queryResult = bioNetDbManager.getNodeQueryExecutor().query(cypher.toString());
        printNodes(queryResult.getResults());
    }

    //-------------------------------------------------------------------------

//    @Test
//    public void tableQuery() throws Exception {
//        Query query = new Query();
//        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
//        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "GENE.name,GENE.id,TRANSCRIPT.id");
//        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
//        System.out.println(cypher);
//        QueryResult<List<Object>> queryResult = bioNetDbManager.table(cypher);
//        printLists(queryResult.getResult());
//    }
//
//    @Test
//    public void tableByCypher() throws Exception {
//        StringBuilder cypher = new StringBuilder();
//        cypher.append("match (g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return g.uid, g.id, t.uid, t.id, g");
//        QueryResult<List<Object>> queryResult = bioNetDbManager.table(cypher.toString());
//        printLists(queryResult.getResult());
//    }

    @Test
    public void networkQuery() throws Exception {
        Query query = new Query();
        query.put(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key(), "GENE:name=\"APOE\"");
        query.put(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key(), "TRANSCRIPT");
        query.put(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key(), "network");
        String cypher = Neo4JQueryParser.parse(query, QueryOptions.empty());
        System.out.println(cypher);
//        DataResult<Network> queryResult = bioNetDbManager.networkQuery(cypher);
//        System.out.println(queryResult.getResults().get(0).toString());
    }

    //-------------------------------------------------------------------------

    @Test
    public void networkByCypher() throws Exception {
        StringBuilder cypher = new StringBuilder();
        cypher.append("match path=(g:GENE)-[r]-(t:TRANSCRIPT) where g.name=\"APOE\" return path");
//        DataResult<Network> queryResult = bioNetDbManager.networkQuery(cypher.toString());
//        System.out.println(queryResult.getResults().get(0).toString());
    }

    //-------------------------------------------------------------------------

    @Test
    public void variantAnalysis() {
        VariantAnalysis variantAnalysis = bioNetDbManager.getVariantAnalysis();
    }

//    @Test
//    public void dominant() throws BioNetDBException, IOException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily1(disorder);
//
//        Query query = new Query();
////        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("panel", "Familial or syndromic hypoparathyroidism,Hereditary haemorrhagic telangiectasia," +
//                "Neurotransmitter disorders," +
//                "Familial Tumours Syndromes of the central & peripheral Nervous system" +
//                "Inherited non-medullary thyroid cancer" +
//                "Cytopaenias and congenital anaemias" +
//                "Ectodermal dysplasia without a known gene mutation" +
//                "Hyperammonaemia" +
//                "Neuro-endocrine Tumours- PCC and PGL" +
//                "Classical tuberous sclerosis" +
//                "Familial hypercholesterolaemia" +
//                "Pain syndromes" +
//                "Congenital myopathy" +
//                "Corneal abnormalities" +
//                "Hydrocephalus" +
//                "Infantile enterocolitis & monogenic inflammatory bowel disease" +
//                "Severe familial anorexia" +
//                "Haematological malignancies for rare disease" +
//                "Long QT syndrome" +
//                "Infantile nystagmus");
//        query.put("gene", "BRCA1,BRCA2");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        excludeAll(query);
//
//        QueryResult<Variant> dominantVariants = bioNetDbManager.getVariantAnalysis().getDominantVariants(pedigree, disorder, query);
//        if (dominantVariants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : dominantVariants.getResult()) {
//                System.out.println(variant.toStringSimple());
//            }
//        }
////        System.out.println(dominantVariants.first());
//    }
//
//    @Test
//    public void recessive() throws BioNetDBException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily1(disorder);
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("chromosome", "3");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        excludeAll(query);
//
//        QueryResult<Variant> variants = bioNetDbManager.getVariantAnalysis().getRecessiveVariants(pedigree, disorder, query);
//        if (variants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : variants.getResult()) {
//                System.out.println(variant.toStringSimple());
//            }
//        }
//    }
//
//    @Test
//    public void xLinkedDominant() throws BioNetDBException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily1(disorder);
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        excludeAll(query);
//
//        QueryResult<Variant> variants = bioNetDbManager.getVariantAnalysis().getXLinkedDominantVariants(pedigree, disorder, query);
//        if (variants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : variants.getResult()) {
//                System.out.println(variant.toStringSimple());
//            }
//        }
//    }
//
//    @Test
//    public void xLinkedRecessive() throws BioNetDBException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily2(disorder);
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        QueryResult<Variant> variants = bioNetDbManager.getVariantAnalysis().getXLinkedRecessiveVariants(pedigree, disorder, query);
//        if (variants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : variants.getResult()) {
//                System.out.println(variant.toStringSimple());
//            }
//        }
//    }
//
//    @Test
//    public void yLinked() throws BioNetDBException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily3(disorder);
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        excludeAll(query);
//
//        QueryResult<Variant> variants = bioNetDbManager.getVariantAnalysis().getYLinkedVariants(pedigree, disorder, query);
//        if (variants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : variants.getResult()) {
//                System.out.println(variant.toStringSimple());
//            }
//        }
//    }
//
//    @Test
//    public void deNovo() throws BioNetDBException, IOException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily3(disorder);
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        excludeAll(query);
//
//        QueryResult<Variant> variants = bioNetDbManager.getVariantAnalysis().getDeNovoVariants(pedigree, query);
//        if (variants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : variants.getResult()) {
//                System.out.println(variant.toStringSimple() + " samples --> "
//                        + variant.getAnnotation().getAdditionalAttributes().get("samples").getAttribute().get(NodeBuilder.SAMPLE));
//            }
//        }
//    }
//
//    @Test
//    public void compoundHeterozygous() throws BioNetDBException, IOException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily3(disorder);
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        QueryResult<Map<String, List<Variant>>> variantMap = bioNetDbManager.getVariantAnalysis().getCompoundHeterozygousVariants(pedigree, query);
//        if (variantMap.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (String key : variantMap.first().keySet()) {
//                System.out.println(key);
//                for (Variant variant : variantMap.first().get(key)) {
//                    System.out.println("\t" + variant.toStringSimple() + " samples --> " + variant.getStudies().get(0).getFiles().get(0)
//                            .getData().get("sampleNames"));
//                }
//            }
//        }
//    }
//
//    @Test
//    public void systemDominant() throws BioNetDBException {
//        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);
//        Pedigree pedigree = getPedigreeFamily3(disorder);
//        ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.AUTOSOMAL_DOMINANT;
//
//        Query query = new Query();
//        query.put("panel", "Familial or syndromic hypoparathyroidism");
//        query.put("gene", "BRCA1,BRCA2");
//        query.put("chromosome", "17");
//        query.put("ct", "missense_variant,stop_lost,intron_variant");
//        query.put("biotype", "protein_coding");
//        query.put("populationFrequencyAlt", "ALL<0.05");
//
//        excludeAll(query);
//
//        QueryResult<Variant> variants = bioNetDbManager.getInterpretationAnalysis().proteinNetworkAnalysis(pedigree, disorder, moi, true, query);
//        if (variants.getResult().size() > 0) {
//            System.out.println("\n");
//            System.out.println("Variants:");
//            for (Variant variant : variants.getResult()) {
//                System.out.println(variant.toStringSimple());
//            }
//        }
//    }
//
//    //-------------------------------------------------------------------------
//    // G E T   P E D I G R E E
//    //-------------------------------------------------------------------------
//
//    private Member healthyFather = new Member().setId("NA12877").setSex(Member.Sex.MALE);
//    private Member illFather = new Member().setId("NA12877").setSex(Member.Sex.MALE);
//
//    private Member healthyMother = new Member().setId("NA12878").setSex(Member.Sex.FEMALE);
//    private Member illMother = new Member().setId("NA12878").setSex(Member.Sex.FEMALE);
//
//    private Member healthyDaughter = new Member().setId("NA12879").setSex(Member.Sex.FEMALE)
//            .setMother(illMother).setFather(healthyFather);
//    private Member illDaughter = new Member().setId("NA12879").setSex(Member.Sex.FEMALE)
//            .setMother(illMother).setFather(healthyFather);
//
//    private Pedigree getPedigreeFamily1(Disorder disorder) {
//        illMother.setDisorders(Collections.singletonList(disorder));
//        illDaughter.setDisorders(Collections.singletonList(disorder));
//
//        Pedigree family1 = new Pedigree()
//                .setMembers(Arrays.asList(healthyFather, illMother, illDaughter))
//                .setDisorders(Collections.singletonList(disorder));
//        family1.setProband(illDaughter);
//
//        return family1;
//    }
//
//    private Pedigree getPedigreeFamily2(Disorder disorder) {
//        illFather.setDisorders(Collections.singletonList(disorder));
//        illMother.setDisorders(Collections.singletonList(disorder));
//        illDaughter.setDisorders(Collections.singletonList(disorder));
//
//        Pedigree family2 = new Pedigree()
//                .setMembers(Arrays.asList(illFather, illMother, illDaughter))
//                .setDisorders(Collections.singletonList(disorder));
//        family2.setProband(illDaughter);
//
//        return family2;
//    }
//
//    private Pedigree getPedigreeFamily3(Disorder disorder) {
//        illFather.setDisorders(Collections.singletonList(disorder));
//
//        Pedigree family3 = new Pedigree()
//                .setMembers(Arrays.asList(illFather, healthyMother, healthyDaughter))
//                .setDisorders(Collections.singletonList(disorder));
//        family3.setProband(illDaughter);
//
//        return family3;
//    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void printNodes(List<Node> nodes) {
        for (Node node : nodes) {
            System.out.println(node.toStringEx());
        }
    }

    private void printLists(List<List<Object>> lists) {
        for (List<Object> list : lists) {
            for (Object item : list) {
                System.out.print(item + ", ");
            }
        }
    }

//    @Test
//    public void getEnsemblGene() throws IOException {
//        String geneId = "ENSG00000164053";
//        String transcriptId = "ENST00000424906";
//
//        // CellBase client
//        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        clientConfiguration.setVersion("v4");
//        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
//        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", "GRCh37", clientConfiguration);
//        GeneClient geneClient = cellBaseClient.getGeneClient();
//
//        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,transcripts.cDnaSequence,annotation.expression");
//        List<String> ids = new ArrayList<>();
//        ids.add(geneId);
//        CellBaseDataResponse<Gene> geneQueryResponse = geneClient.get(ids, options);
//        for (CellBaseDataResult<Gene> responses : geneQueryResponse.getResponses()) {
//            for (Gene gene : responses.getResults()) {
//                System.out.println(gene.getId() + ", " + gene.getName());
//                for (Transcript transcript : gene.getTranscripts()) {
//                    System.out.println("\t" + transcript.getId() + ", " + transcript.getName());
//                    if (transcript.getId().equals(transcriptId)) {
//                        System.out.println("\t\tFOUND !!!!");
//                    }
//                }
//            }
//        }
//    }
}