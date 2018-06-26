package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.DbReferenceType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.metadata.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class CsvInfo {
    public static final String SEPARATOR = ",";
    private static final String MISSING_VALUE = "-";

    private long uid;
    private Path inputPath;
    private Path outputPath;

    private List<String> sampleNames;
    private Set<String> formatFields;
    private Set<String> infoFields;

    private Map<String, PrintWriter> csvWriters;
    private Map<String, PrintWriter> csvAnnotatedWriters;
    private Map<String, List<String>> nodeAttributes;

    private RocksDbManager rocksDbManager;
    private RocksDB rocksDB;

    private RocksDB geneRocksDB;
    private RocksDB proteinRocksDB;
    private RocksDB miRnaRocksDB;

    private ObjectMapper mapper;
    private ObjectReader geneReader;
    private ObjectReader proteinReader;
    private ObjectWriter objWriter;

    protected static Logger logger;

    public enum BioPAXRelation {
        COMPONENT_OF_COMPLEX___COMPLEX___COMPLEX("COMPONENT_OF_COMPLEX___COMPLEX___COMPLEX"),
        COMPONENT_OF_COMPLEX___PROTEIN___COMPLEX("COMPONENT_OF_COMPLEX___PROTEIN___COMPLEX"),
        COMPONENT_OF_COMPLEX___SMALL_MOLECULE___COMPLEX("COMPONENT_OF_COMPLEX___SMALL_MOLECULE___COMPLEX"),
        COMPONENT_OF_COMPLEX___UNDEFINED___COMPLEX("COMPONENT_OF_COMPLEX___UNDEFINED___COMPLEX"),
        COMPONENT_OF_COMPLEX___DNA___COMPLEX("COMPONENT_OF_COMPLEX___DNA___COMPLEX"),
        COMPONENT_OF_COMPLEX___RNA___COMPLEX("COMPONENT_OF_COMPLEX___RNA___COMPLEX"),

        CELLULAR_LOCATION___PROTEIN___CELLULAR_LOCATION("CELLULAR_LOCATION___PROTEIN___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___RNA___CELLULAR_LOCATION("CELLULAR_LOCATION___RNA___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___COMPLEX___CELLULAR_LOCATION("CELLULAR_LOCATION___COMPLEX___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___SMALL_MOLECULE___CELLULAR_LOCATION("CELLULAR_LOCATION___SMALL_MOLECULE___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___DNA___CELLULAR_LOCATION("CELLULAR_LOCATION___DNA___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___UNDEFINED___CELLULAR_LOCATION("CELLULAR_LOCATION___UNDEFINED___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___REACTION___CELLULAR_LOCATION("CELLULAR_LOCATION___REACTION___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___REGULATION___CELLULAR_LOCATION("CELLULAR_LOCATION___REGULATION___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___CATALYSIS___CELLULAR_LOCATION("CELLULAR_LOCATION___CATALYSIS___CELLULAR_LOCATION"),

        REACTANT___REACTION___SMALL_MOLECULE("REACTANT___REACTION___SMALL_MOLECULE"),
        REACTANT___REACTION___PROTEIN("REACTANT___REACTION___PROTEIN"),
        REACTANT___REACTION___COMPLEX("REACTANT___REACTION___COMPLEX"),
        REACTANT___REACTION___UNDEFINED("REACTANT___REACTION___UNDEFINED"),
        REACTANT___REACTION___DNA("REACTANT___REACTION___DNA"),
        REACTANT___REACTION___RNA("REACTANT___REACTION___RNA"),

        PRODUCT___REACTION___PROTEIN("PRODUCT___REACTION___PROTEIN"),
        PRODUCT___REACTION___SMALL_MOLECULE("PRODUCT___REACTION___SMALL_MOLECULE"),
        PRODUCT___REACTION___COMPLEX("PRODUCT___REACTION___COMPLEX"),
        PRODUCT___REACTION___UNDEFINED("PRODUCT___REACTION___UNDEFINED"),
        PRODUCT___REACTION___RNA("PRODUCT___REACTION___RNA"),

        CONTROLLER___CATALYSIS___PROTEIN("CONTROLLER___CATALYSIS___PROTEIN"),
        CONTROLLER___CATALYSIS___COMPLEX("CONTROLLER___CATALYSIS___COMPLEX"),
        CONTROLLER___CATALYSIS___UNDEFINED("CONTROLLER___CATALYSIS___UNDEFINED"),
        CONTROLLER___REGULATION___COMPLEX("CONTROLLER___REGULATION___COMPLEX"),
        CONTROLLER___REGULATION___PROTEIN("CONTROLLER___REGULATION___PROTEIN"),
        CONTROLLER___REGULATION___UNDEFINED("CONTROLLER___REGULATION___UNDEFINED"),
        CONTROLLER___REGULATION___SMALL_MOLECULE("CONTROLLER___REGULATION___SMALL_MOLECULE"),
        CONTROLLER___REGULATION___RNA("CONTROLLER___REGULATION___RNA"),

        CONTROLLED___REGULATION___CATALYSIS("CONTROLLED___REGULATION___CATALYSIS"),
        CONTROLLED___CATALYSIS___REACTION("CONTROLLED___CATALYSIS___REACTION"),
        CONTROLLED___REGULATION___REACTION("CONTROLLED___REGULATION___REACTION"),
        CONTROLLED___REGULATION___PATHWAY("CONTROLLED___REGULATION___PATHWAY"),

        COMPONENT_OF_PATHWAY___PATHWAY___PATHWAY("COMPONENT_OF_PATHWAY___PATHWAY___PATHWAY"),
        COMPONENT_OF_PATHWAY___REACTION___PATHWAY("COMPONENT_OF_PATHWAY___REACTION___PATHWAY"),

        PATHWAY_NEXT_STEP___PATHWAY___PATHWAY("PATHWAY_NEXT_STEP___PATHWAY___PATHWAY"),
        PATHWAY_NEXT_STEP___CATALYSIS___REACTION("PATHWAY_NEXT_STEP___CATALYSIS___REACTION"),
        PATHWAY_NEXT_STEP___REACTION___REACTION("PATHWAY_NEXT_STEP___REACTION___REACTION"),
        PATHWAY_NEXT_STEP___REACTION___CATALYSIS("PATHWAY_NEXT_STEP___REACTION___CATALYSIS"),
        PATHWAY_NEXT_STEP___CATALYSIS___CATALYSIS("PATHWAY_NEXT_STEP___CATALYSIS___CATALYSIS"),
        PATHWAY_NEXT_STEP___REACTION___REGULATION("PATHWAY_NEXT_STEP___REACTION___REGULATION"),
        PATHWAY_NEXT_STEP___REACTION___PATHWAY("PATHWAY_NEXT_STEP___REACTION___PATHWAY"),
        PATHWAY_NEXT_STEP___REGULATION___REACTION("PATHWAY_NEXT_STEP___REGULATION___REACTION"),
        PATHWAY_NEXT_STEP___REGULATION___PATHWAY("PATHWAY_NEXT_STEP___REGULATION___PATHWAY"),
        PATHWAY_NEXT_STEP___REGULATION___CATALYSIS("PATHWAY_NEXT_STEP___REGULATION___CATALYSIS"),
        PATHWAY_NEXT_STEP___REGULATION___REGULATION("PATHWAY_NEXT_STEP___REGULATION___REGULATION"),
        PATHWAY_NEXT_STEP___CATALYSIS___REGULATION("PATHWAY_NEXT_STEP___CATALYSIS___REGULATION"),
        PATHWAY_NEXT_STEP___CATALYSIS___PATHWAY("PATHWAY_NEXT_STEP___CATALYSIS___PATHWAY"),
        PATHWAY_NEXT_STEP___PATHWAY___CATALYSIS("PATHWAY_NEXT_STEP___PATHWAY___CATALYSIS"),
        PATHWAY_NEXT_STEP___PATHWAY___REACTION("PATHWAY_NEXT_STEP___PATHWAY___REACTION"),
        PATHWAY_NEXT_STEP___PATHWAY___REGULATION("PATHWAY_NEXT_STEP___PATHWAY___REGULATION"),

        XREF___PROTEIN___XREF("XREF___PROTEIN___XREF"),
        XREF___RNA___XREF("XREF___RNA___XREF"),

        TARGET_GENE___MIRNA___GENE("TARGET_GENE___MIRNA___GENE"),

        IS___DNA___GENE("IS___DNA___GENE"),
        IS___RNA___MIRNA("IS___DNA___GENE");

        private final String relation;

        BioPAXRelation(String relation) {
            this.relation = relation;
        }

        public List<BioPAXRelation> getAll() {
            List<BioPAXRelation> list = new ArrayList<>();
            return list;
        }
    }

    public CsvInfo(Path inputPath, Path outputPath) {
        uid = 1;
        //this.bioPAXImporter = new Neo4jBioPaxImporter()

        this.inputPath = inputPath;
        this.outputPath = outputPath;

        sampleNames = new ArrayList<>();
        infoFields = new HashSet<>();
        formatFields = new HashSet<>();


        csvWriters = new HashMap<>();
        csvAnnotatedWriters = new HashMap<>();
        nodeAttributes = createNodeAttributes();

        rocksDbManager = new RocksDbManager();
        rocksDB = this.rocksDbManager.getDBConnection(outputPath.toString() + "/rocksdb", true);

        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        geneReader = mapper.reader(Gene.class);
        proteinReader = mapper.reader(Entry.class);
        objWriter = mapper.writer();

        logger = LoggerFactory.getLogger(this.getClass());
    }

    public long getAndIncUid() {
        long ret = uid;
        uid++;
        return ret;
    }

    public void openCSVFiles() throws IOException {
        PrintWriter pw;
        String filename;

        // CSV files for nodes
        for (Node.Type type: Node.Type.values()) {
            filename = type.toString() + ".csv";

            pw = new PrintWriter(outputPath + "/" + filename);
            csvWriters.put(type.toString(), pw);

            if (ListUtils.isNotEmpty(nodeAttributes.get(type.toString()))) {
                pw.println(getNodeHeaderLine(nodeAttributes.get(type.toString())));
            }
        }

        // CSV files for relationships
        for (Relation.Type type: Relation.Type.values()) {
            if (type.toString().contains("__")) {
                filename = type.toString() + ".csv";
//                pw = new PrintWriter(new BufferedWriter(new FileWriter(outputPath + "/" + filename, !header)));
                pw = new PrintWriter(outputPath + "/" + filename);
                csvWriters.put(type.toString(), pw);

                if (type != Relation.Type.VARIANT__VARIANT_CALL
                        && type != Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO
                        && type != Relation.Type.VARIANT_FILE_INFO__FILE
                        && type != Relation.Type.SAMPLE__VARIANT_CALL) {
                    pw.println(getRelationHeaderLine(type.toString()));
                }
            }
        }

        // CSV files for BioPAX relationships
        for (BioPAXRelation type: BioPAXRelation.values()) {
            filename = type.toString() + ".csv";
            pw = new PrintWriter(outputPath + "/" + filename);
            csvWriters.put(type.toString(), pw);

            pw.println(getBioPAXRelationHeaderLine(type.toString()));
        }
    }

    public void close() throws FileNotFoundException {
        List<Map<String, PrintWriter>> writerMaps = new ArrayList<>();
        writerMaps.add(csvWriters);
        writerMaps.add(csvAnnotatedWriters);

        for (Map<String, PrintWriter> writerMap: writerMaps) {
            if (MapUtils.isNotEmpty(writerMap)) {
                Iterator<PrintWriter> iterator = writerMap.values().iterator();
                while (iterator.hasNext()) {
                    iterator.next().close();
                }
            }
        }
    }

    public void openMetadataFile(File metafile) throws IOException {
        BufferedReader bufferedReader = FileUtils.newBufferedReader(metafile.toPath());
        String metadata = bufferedReader.readLine();
        bufferedReader.close();

        //FileUtils.readFully(new BufferedReader(new FileReader(metafile.getAbsolutePath())));
//        String metadata = FileUtils.readFully(new BufferedReader(new FileReader(metafile.getAbsolutePath())));
        ObjectMapper mapper = new ObjectMapper();
        VariantMetadata variantMetadata = mapper.readValue(metadata, VariantMetadata.class);
        if (ListUtils.isNotEmpty(variantMetadata.getStudies())) {
            VariantStudyMetadata variantStudyMetadata = variantMetadata.getStudies().get(0);
            for (Individual individual: variantStudyMetadata.getIndividuals()) {
                sampleNames.add(individual.getSamples().get(0).getId());
            }

            if (ListUtils.isNotEmpty(variantStudyMetadata.getFiles())) {
                for (VariantFileMetadata variantFileMetadata: variantStudyMetadata.getFiles()) {
                    if (StringUtils.isNotEmpty(variantFileMetadata.getId())) {
                        Long fileUid = getLong(variantFileMetadata.getId());
                        if (fileUid == null) {
                            // File node
                            Node n = new Node(getAndIncUid(), variantFileMetadata.getId(), variantFileMetadata.getPath(),
                                    Node.Type.FILE);
                            csvWriters.get(Node.Type.FILE.toString()).println(nodeLine(n));
                            putLong(variantFileMetadata.getId(), n.getUid());
                        }
                    }

                    if (variantFileMetadata.getHeader() != null) {
                        for (VariantFileHeaderComplexLine line: variantFileMetadata.getHeader().getComplexLines()) {
                            if ("INFO".equals(line.getKey())) {
                                infoFields.add(line.getId());
                            } else if ("FORMAT".equals(line.getKey())) {
                                formatFields.add(line.getId());
                            }
                        }
                    }

                }
            }
        }

        // Variant call
        String strType;
        List<String> attrs = new ArrayList<>();
        attrs.add("variantCallId");
        Iterator<String> iterator = formatFields.iterator();
        while (iterator.hasNext()) {
            attrs.add(iterator.next());
        }
        strType = Node.Type.VARIANT_CALL.toString();
        nodeAttributes.put(strType, attrs);
        csvWriters.get(strType).println(getNodeHeaderLine(attrs));
        strType = Relation.Type.VARIANT__VARIANT_CALL.toString();
        csvWriters.get(strType).println(getRelationHeaderLine(strType));
        strType = Relation.Type.SAMPLE__VARIANT_CALL.toString();
        csvWriters.get(strType).println(getRelationHeaderLine(strType));

        // Variant file info
        attrs = new ArrayList<>();
        attrs.add("variantFileInfoId");
        iterator = infoFields.iterator();
        while (iterator.hasNext()) {
            attrs.add(iterator.next());
        }
        strType = Node.Type.VARIANT_FILE_INFO.toString();
        nodeAttributes.put(strType, attrs);
        csvWriters.get(strType).println(getNodeHeaderLine(attrs));
        strType = Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString();
        csvWriters.get(strType).println(getRelationHeaderLine(strType));
        strType = Relation.Type.VARIANT_FILE_INFO__FILE.toString();
        csvWriters.get(strType).println(getRelationHeaderLine(strType));
    }

    public Gene getGene(String id) {
        Gene gene = null;
        String internalId = rocksDbManager.getString(id, geneRocksDB);
        if (StringUtils.isNotEmpty(internalId)) {
            try {
                gene = geneReader.readValue(rocksDbManager.getString(internalId, geneRocksDB));
            } catch (IOException e) {
                logger.info("Error parsing gene {}: {}", id, e.getMessage());
                gene = null;
            }
        }

        return gene;
    }

    public Entry getProtein(String id) {
        Entry protein = null;
        String internalId = rocksDbManager.getString(id, proteinRocksDB);
        if (StringUtils.isNotEmpty(internalId)) {
            try {
                protein = proteinReader.readValue(rocksDbManager.getString(internalId, proteinRocksDB));
            } catch (IOException e) {
                logger.info("Error parsing protein {}: {}", id, internalId);
                logger.info(e.getMessage());
                protein = null;
            }
        }

        return protein;
    }

    public String getMiRnaInfo(String id) {
        String info = rocksDbManager.getString(id, miRnaRocksDB);
        if (StringUtils.isNotEmpty(info)) {
            return id + ":" + info;
        }

        return info;
    }

    public Long getLong(String key) {
        return rocksDbManager.getLong(key, rocksDB);
    }

    public String getString(String key) {
        return rocksDbManager.getString(key, rocksDB);
    }

    public void putLong(String key, long value) {
        rocksDbManager.putLong(key, value, rocksDB);
    }

    public void putString(String key, String value) {
        rocksDbManager.putString(key, value, rocksDB);
    }

    private String getNodeHeaderLine(List<String> attrs) {
        StringBuilder sb = new StringBuilder();
        sb.append("uid:ID(").append(attrs.get(0)).append(")");
        for (int i = 1; i < attrs.size(); i++) {
            sb.append(SEPARATOR).append(attrs.get(i));
        }
        return sb.toString();
    }

    private String getRelationHeaderLine(String type) {
        StringBuilder sb = new StringBuilder();
        String[] split = type.split("__");
        sb.append(":START_ID(").append(nodeAttributes.get(split[0]).get(0)).append("),:END_ID(")
                .append(nodeAttributes.get(split[1]).get(0)).append(")");
        return sb.toString();
    }

    private String getBioPAXRelationHeaderLine(String type) {
        StringBuilder sb = new StringBuilder();
        String[] split = type.split("___");
        sb.append(":START_ID(").append(nodeAttributes.get(split[1]).get(0)).append("),:END_ID(")
                .append(nodeAttributes.get(split[2]).get(0)).append(")");
        if (type.equals(BioPAXRelation.TARGET_GENE___MIRNA___GENE.toString())) {
            sb.append(",evidence");
        }
        return sb.toString();
    }

    // Debug purposes
    private Set<String> notdefined = new HashSet<>();

    public String nodeLine(Node node) {
        List<String> attrs = nodeAttributes.get(node.getType().toString());
        StringBuilder sb = new StringBuilder();
        if (ListUtils.isEmpty(attrs)) {
            if (!notdefined.contains(node.getType().toString())) {
                System.out.println("Attributes not defined for " + node.getType());
                notdefined.add(node.getType().toString());
            }
        } else {
            sb.append(node.getUid()).append(SEPARATOR);
            String value = cleanString(node.getId());
            sb.append(StringUtils.isEmpty(value) ? MISSING_VALUE : value).append(SEPARATOR);
            value = cleanString(node.getName());
            sb.append(StringUtils.isEmpty(value) ? MISSING_VALUE : value);
            for (int i = 3; i < attrs.size(); i++) {
                value = cleanString(node.getAttributes().getString(attrs.get(i)));
                sb.append(SEPARATOR).append(StringUtils.isEmpty(value) ? MISSING_VALUE : value);
            }
        }
        return sb.toString();
    }

    public String relationLine(long startUid, long endUid) {
        StringBuilder sb = new StringBuilder();
        sb.append(startUid).append(SEPARATOR).append(endUid);
        return sb.toString();
    }

    private String cleanString(String input) {
        if (StringUtils.isNotEmpty(input)) {
            return input.replace(",", ";").replace("\"", "");
        } else {
            return null;
        }
    }

    public void indexingGenes(Path genePath, Path indexPath) throws IOException {
        RocksDbManager rocksDbManager = new RocksDbManager();

        if (indexPath.toFile().exists()) {
            geneRocksDB = rocksDbManager.getDBConnection(indexPath.toString(), false);
            logger.info("\tGene index already created!");
            return;
        }

        geneRocksDB = rocksDbManager.getDBConnection(indexPath.toString(), false);

        BufferedReader reader = org.opencb.commons.utils.FileUtils.newBufferedReader(genePath);
        String jsonGene = reader.readLine();
        long geneCounter = 0, transcrCounter = 0;
        while (jsonGene != null) {
            Gene gene = geneReader.readValue(jsonGene);
            if (StringUtils.isNotEmpty(gene.getId()) || StringUtils.isNotEmpty(gene.getName())) {
                geneCounter++;
                if (geneCounter % 5000 == 0) {
                    logger.info("Indexing {} genes...", geneCounter);
                }
                String gKey = "g." + geneCounter;
                rocksDbManager.putString(gKey, jsonGene, geneRocksDB);
                if (StringUtils.isNotEmpty(gene.getId())) {
                    rocksDbManager.putString(gene.getId(), gKey, geneRocksDB);
                }
                if (StringUtils.isNotEmpty(gene.getName())) {
                    rocksDbManager.putString(gene.getName(), gKey, geneRocksDB);
                }

                if (ListUtils.isNotEmpty(gene.getTranscripts())) {
                    for (Transcript transcr : gene.getTranscripts()) {
                        if (StringUtils.isNotEmpty(transcr.getId()) || org.apache.commons.lang3.StringUtils.isNotEmpty(transcr.getName())) {
//                            transcrCounter++;
//                            key = "t." + geneCounter;
//                            String jsonTranscr = objWriter.writeValueAsString(transcr);
//                            rocksDbManager.putString(key, jsonTranscr, geneRocksDB);
//                            if (StringUtils.isNotEmpty(transcr.getId())) {
//                                rocksDbManager.putString(transcr.getId(), key, geneRocksDB);
//                            }
//                            if (StringUtils.isNotEmpty(transcr.getName())) {
//                                rocksDbManager.putString(transcr.getName(), key, geneRocksDB);
//                            }
                            if (ListUtils.isNotEmpty(transcr.getXrefs())) {
                                for (Xref xref: transcr.getXrefs()) {
//                                    Node node = NodeBuilder.newNode(getAndIncUid(), xref);
                                    rocksDbManager.putString(xref.getId(), gKey, geneRocksDB);
                                }
                            }
                        }
                    }
                }
            } else {
                logger.info("Missing gene ID/name from JSON file");
            }


            // Next line
            jsonGene = reader.readLine();
        }
        logger.info("Indexing {} genes. Done.", geneCounter);

        reader.close();
    }

    public void indexingProteins(Path proteinPath, Path indexPath) throws IOException {
        RocksDbManager rocksDbManager = new RocksDbManager();

        if (indexPath.toFile().exists()) {
            proteinRocksDB = rocksDbManager.getDBConnection(indexPath.toString(), false);
            logger.info("\tProtein index already created!");
            return;
        }

        proteinRocksDB = rocksDbManager.getDBConnection(indexPath.toString(), false);

        BufferedReader reader = org.opencb.commons.utils.FileUtils.newBufferedReader(proteinPath);
        String jsonProtein = reader.readLine();
        long proteinCounter = 0;
        while (jsonProtein != null) {
            Entry protein = proteinReader.readValue(jsonProtein);
            if (ListUtils.isNotEmpty(protein.getAccession()) || ListUtils.isNotEmpty(protein.getName())) {
                proteinCounter++;
                if (proteinCounter % 5000 == 0) {
                    logger.info("Indexing {} proteins...", proteinCounter);
                }
                String pKey = "p." + proteinCounter;
                rocksDbManager.putString(pKey, jsonProtein, proteinRocksDB);

                if (ListUtils.isNotEmpty(protein.getAccession())) {
                    for (String acc: protein.getAccession()) {
                        rocksDbManager.putString(acc, pKey, proteinRocksDB);
                    }
                }

                if (ListUtils.isNotEmpty(protein.getName())) {
                    for (String name: protein.getName()) {
                        rocksDbManager.putString(name, pKey, proteinRocksDB);
                    }
                }

                if (ListUtils.isNotEmpty(protein.getDbReference())) {
                    for (DbReferenceType dbRef: protein.getDbReference()) {
                        rocksDbManager.putString(dbRef.getId(), pKey, proteinRocksDB);
                    }
                }
            } else {
                logger.info("Missing protein accession/name from JSON file");
            }

            // Next line
            jsonProtein = reader.readLine();
        }
        logger.info("Indexing {} proteins. Done.", proteinCounter);

        reader.close();
    }

    public void indexingMiRnas(Path miRnaPath, Path indexPath) throws IOException {
        RocksDbManager rocksDbManager = new RocksDbManager();

        if (indexPath.toFile().exists()) {
            logger.info("\tmiRNA index already created!");
            miRnaRocksDB = rocksDbManager.getDBConnection(indexPath.toString(), false);
            return;
        }
        miRnaRocksDB = rocksDbManager.getDBConnection(indexPath.toString(), false);

        BufferedReader reader = org.opencb.commons.utils.FileUtils.newBufferedReader(miRnaPath);
        String line = reader.readLine();
        String[] fields = line.split(",");
        int indexMiRnaId = -1, indexTargetGene = -1, indexEvidence = -1;
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].equals("miRNA")) {
                indexMiRnaId = i;
            } else if (fields[i].equals("Target Gene")) {
                indexTargetGene = i;
            } else if (fields[i].equals("Support Type")) {
                indexEvidence = i;
            }
        }
        if (indexMiRnaId == -1 || indexTargetGene == -1 || indexEvidence == -1) {
            logger.error("MiRNA file header is invalid, check your the header fields: miRNA, Target Gene and Support"
                    + " Type");
            return;
        }

        // Main loop, read miRNA CSV file
        String miRna, targetGene, evidence;
        line = reader.readLine();
        long miRnaCounter = 0;
        while (line != null) {
                miRnaCounter++;
                if (miRnaCounter % 5000 == 0) {
                    logger.info("Indexing {} miRNAs...", miRnaCounter);
                }
            fields = line.split(",");
            miRna = fields[indexMiRnaId];
            targetGene = fields[indexTargetGene];
            evidence = fields[indexEvidence];
            if (StringUtils.isNotEmpty(miRna)) {
                rocksDbManager.putString(miRna, targetGene + ":" + evidence, miRnaRocksDB);
            }

            // Next line
            line = reader.readLine();
        }
        logger.info("Indexing {} miRNAs. Done.", miRnaCounter);

        reader.close();
    }

    private Map<String, List<String>> createNodeAttributes() {
        List<String> attrs;
        Map<String, List<String>> nodeAttributes = new HashMap<>();

        // Variant
        attrs = Arrays.asList("variantId", "id", "name", "alternativeNames", "chromosome", "start", "end", "strand",
                "reference", "alternate", "type");
        nodeAttributes.put(Node.Type.VARIANT.toString(), new ArrayList<>(attrs));

        // Population frequency
        attrs = Arrays.asList("popFreqId", "id", "name", "study", "population", "refAlleleFreq", "altAlleleFreq");
        nodeAttributes.put(Node.Type.POPULATION_FREQUENCY.toString(), new ArrayList<>(attrs));

        // Conservation
        attrs = Arrays.asList("consId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Type.CONSERVATION.toString(), new ArrayList<>(attrs));

        // Functional score
        attrs = Arrays.asList("funcScoreId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Type.FUNCTIONAL_SCORE.toString(), new ArrayList<>(attrs));

        // Trait association
        attrs = Arrays.asList("traitId", "id", "name", "url", "heritableTraits", "source", "alleleOrigin");
        nodeAttributes.put(Node.Type.TRAIT_ASSOCIATION.toString(), new ArrayList<>(attrs));

        // Consequence type
        attrs = Arrays.asList("consTypeId", "id", "name", "study", "biotype", "cdnaPosition", "cdsPosition", "codon",
                "strand", "gene", "transcript", "transcriptAnnotationFlags", "exonOverlap");
        nodeAttributes.put(Node.Type.CONSEQUENCE_TYPE.toString(), new ArrayList<>(attrs));

        // Protein variant annotation
        attrs = Arrays.asList("protVarAnnoId", "id", "name", "position", "reference", "alternate",
                "functionalDescription");
        nodeAttributes.put(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        // Gene
        attrs = Arrays.asList("geneId", "id", "name", "biotype", "chromosome", "start", "end", "strand", "description",
                "source", "status");
        nodeAttributes.put(Node.Type.GENE.toString(), new ArrayList<>(attrs));

        // Drug
        attrs = Arrays.asList("drugId", "id", "name", "source", "type", "studyType");
        nodeAttributes.put(Node.Type.DRUG.toString(), new ArrayList<>(attrs));

        // Disease
        attrs = Arrays.asList("diseaseId", "id", "name", "hpo", "numberOfPubmeds", "score", "source", "sources",
                "associationType");
        nodeAttributes.put(Node.Type.DISEASE.toString(), new ArrayList<>(attrs));

        // Transcript
        attrs = Arrays.asList("transcriptId", "id", "name", "proteinId", "biotype", "chromosome", "start", "end",
                "strand", "status", "cdnaCodingStart", "cdnaCodingEnd", "genomicCodingStart", "genomicCodingEnd",
                "cdsLength", "description", "annotationFlags");
        nodeAttributes.put(Node.Type.TRANSCRIPT.toString(), new ArrayList<>(attrs));

        // Target transcript
        attrs = Arrays.asList("rnaId", "id", "name", "evidence");
        nodeAttributes.put(Node.Type.TARGET_TRANSCRIPT.toString(), new ArrayList<>(attrs));

        // miRNA
        attrs = Arrays.asList("mirnaId", "id", "name");
        nodeAttributes.put(Node.Type.MIRNA.toString(), new ArrayList<>(attrs));

        // TFBS
        attrs = Arrays.asList("tfbsId", "id", "name", "chromosome", "start", "end", "strand", "relativeStart",
                "relativeEnd", "score", "pwm");
        nodeAttributes.put(Node.Type.TFBS.toString(), new ArrayList<>(attrs));

        // Xref
        attrs = Arrays.asList("xrefId", "id", "name", "dbName", "dbDisplayName", "description");
        nodeAttributes.put(Node.Type.XREF.toString(), new ArrayList<>(attrs));

        // Protein
        attrs = Arrays.asList("protId", "id", "name", "accession", "dataset", "proteinExistence", "evidence");
        nodeAttributes.put(Node.Type.PROTEIN.toString(), new ArrayList<>(attrs));

        // Protein keyword
        attrs = Arrays.asList("kwId", "id", "name", "evidence");
        nodeAttributes.put(Node.Type.PROTEIN_KEYWORD.toString(), new ArrayList<>(attrs));

        // Protein feature
        attrs = Arrays.asList("protFeatureId", "id", "name", "evidence", "location_position", "location_begin",
                "location_end", "description");
        nodeAttributes.put(Node.Type.PROTEIN_FEATURE.toString(), new ArrayList<>(attrs));

        // File
        attrs = Arrays.asList("fileId", "id", "name");
        nodeAttributes.put(Node.Type.FILE.toString(), new ArrayList<>(attrs));

        // Sample
        attrs = Arrays.asList("sampleId", "id", "name");
        nodeAttributes.put(Node.Type.SAMPLE.toString(), new ArrayList<>(attrs));

        // SO
        attrs = Arrays.asList("soId", "id", "name");
        nodeAttributes.put(Node.Type.SO.toString(), new ArrayList<>(attrs));

        // Protein variant annotation
        attrs = Arrays.asList("protVarAnnoId", "id", "name");
        nodeAttributes.put(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        // Substitution score
        attrs = Arrays.asList("substScoreId", "id", "name", "score");
        nodeAttributes.put(Node.Type.SUBSTITUTION_SCORE.toString(), new ArrayList<>(attrs));

        //
        // BIO PAX: nodes
        //

        // Cellular location
        attrs = Arrays.asList("cellularLocationId", "id", "name");
        nodeAttributes.put(Node.Type.CELLULAR_LOCATION.toString(), new ArrayList<>(attrs));

        // Pathway
        attrs = Arrays.asList("pathwayId", "id", "name");
        nodeAttributes.put(Node.Type.PATHWAY.toString(), new ArrayList<>(attrs));

        // Small molecule
        attrs = Arrays.asList("smallMoleculeId", "id", "name");
        nodeAttributes.put(Node.Type.SMALL_MOLECULE.toString(), new ArrayList<>(attrs));

        // RNA
        attrs = Arrays.asList("rnaId", "id", "name", "evidence");
        nodeAttributes.put(Node.Type.RNA.toString(), new ArrayList<>(attrs));

        // catalysis
        attrs = Arrays.asList("catalysisId", "id", "name");
        nodeAttributes.put(Node.Type.CATALYSIS.toString(), new ArrayList<>(attrs));

        // complex
        attrs = Arrays.asList("complexId", "id", "name");
        nodeAttributes.put(Node.Type.COMPLEX.toString(), new ArrayList<>(attrs));

        // reaction
        attrs = Arrays.asList("reactionId", "id", "name");
        nodeAttributes.put(Node.Type.REACTION.toString(), new ArrayList<>(attrs));

        // DNA
        attrs = Arrays.asList("dnaId", "id", "name");
        nodeAttributes.put(Node.Type.DNA.toString(), new ArrayList<>(attrs));

        // Undefined
        attrs = Arrays.asList("undefinedId", "id", "name");
        nodeAttributes.put(Node.Type.UNDEFINED.toString(), new ArrayList<>(attrs));

        // Regulation
        attrs = Arrays.asList("regulationId", "id", "name");
        nodeAttributes.put(Node.Type.REGULATION.toString(), new ArrayList<>(attrs));

        return nodeAttributes;
    }

    public long getUid() {
        return uid;
    }

    public CsvInfo setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public static String getSEPARATOR() {
        return SEPARATOR;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public CsvInfo setOutputPath(Path outputPath) {
        this.outputPath = outputPath;
        return this;
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public CsvInfo setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    public Map<String, PrintWriter> getCsvWriters() {
        return csvWriters;
    }

    public CsvInfo setCsvWriters(Map<String, PrintWriter> csvWriters) {
        this.csvWriters = csvWriters;
        return this;
    }

    public Map<String, PrintWriter> getCsvAnnotatedWriters() {
        return csvAnnotatedWriters;
    }

    public CsvInfo setCsvAnnotatedWriters(Map<String, PrintWriter> csvAnnotatedWriters) {
        this.csvAnnotatedWriters = csvAnnotatedWriters;
        return this;
    }

    public Map<String, List<String>> getNodeAttributes() {
        return nodeAttributes;
    }

    public CsvInfo setNodeAttributes(Map<String, List<String>> nodeAttributes) {
        this.nodeAttributes = nodeAttributes;
        return this;
    }

    public Set<String> getFormatFields() {
        return formatFields;
    }

    public CsvInfo setFormatFields(Set<String> formatFields) {
        this.formatFields = formatFields;
        return this;
    }

    public Set<String> getInfoFields() {
        return infoFields;
    }

    public CsvInfo setInfoFields(Set<String> infoFields) {
        this.infoFields = infoFields;
        return this;
    }

    public RocksDbManager getRocksDbManager() {
        return rocksDbManager;
    }

    public CsvInfo setRocksDbManager(RocksDbManager rocksDbManager) {
        this.rocksDbManager = rocksDbManager;
        return this;
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    public CsvInfo setRocksDB(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        return this;
    }
}
