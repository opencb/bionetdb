package org.opencb.bionetdb.lib.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v202003jaxb.Entry;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.metadata.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.opencb.bionetdb.lib.utils.cache.GeneCache;
import org.opencb.bionetdb.lib.utils.cache.ProteinCache;
import org.opencb.commons.utils.FileUtils;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.*;

import static org.opencb.bionetdb.core.models.network.Node.Type.*;
import static org.opencb.bionetdb.lib.utils.Utils.PREFIX_ATTRIBUTES;

public class CsvInfo {
    public static final String SEPARATOR = "\t";
    public static final String ARRAY_SEPARATOR = "|";
    public static final String MISSING_VALUE = ""; //"-";

    private static final String MIRNA_ROCKSDB = "mirnas.rocksdb";

    private long uid;
    private Path inputPath;
    private Path outputPath;

//    private List<String> sampleIds;

    private Map<String, PrintWriter> csvWriters;
    private Map<String, PrintWriter> csvAnnotatedWriters;
    private Map<String, List<String>> nodeAttributes;
    private Set<String> noAttributes;

    private RocksDbManager rocksDbManager;
    private RocksDB uidRocksDb;

    private GeneCache geneCache;
    private ProteinCache proteinCache;

    private RocksDB miRnaRocksDb;

    private ObjectMapper mapper;
    private ObjectReader geneReader;
    private ObjectReader proteinReader;
    private ObjectWriter objWriter;

    protected static Logger logger;

    public enum BioPAXRelation {
        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___PHYSICAL_ENTITY_COMPLEX___PHYSICAL_ENTITY_COMPLEX
                ("COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___PHYSICAL_ENTITY_COMPLEX___PHYSICAL_ENTITY_COMPLEX"),
        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___PROTEIN___PHYSICAL_ENTITY_COMPLEX
                ("COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___PROTEIN___PHYSICAL_ENTITY_COMPLEX"),
        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___SMALL_MOLECULE___PHYSICAL_ENTITY_COMPLEX
                ("COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___SMALL_MOLECULE___PHYSICAL_ENTITY_COMPLEX"),
        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___UNDEFINED___PHYSICAL_ENTITY_COMPLEX
                ("COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___UNDEFINED___PHYSICAL_ENTITY_COMPLEX"),
        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___DNA___PHYSICAL_ENTITY_COMPLEX
                ("COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___DNA___PHYSICAL_ENTITY_COMPLEX"),
        COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___RNA___PHYSICAL_ENTITY_COMPLEX
                ("COMPONENT_OF_PHYSICAL_ENTITY_COMPLEX___RNA___PHYSICAL_ENTITY_COMPLEX"),

        CELLULAR_LOCATION___PROTEIN___CELLULAR_LOCATION("CELLULAR_LOCATION___PROTEIN___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___RNA___CELLULAR_LOCATION("CELLULAR_LOCATION___RNA___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___PHYSICAL_ENTITY_COMPLEX___CELLULAR_LOCATION("CELLULAR_LOCATION___PHYSICAL_ENTITY_COMPLEX___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___SMALL_MOLECULE___CELLULAR_LOCATION("CELLULAR_LOCATION___SMALL_MOLECULE___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___DNA___CELLULAR_LOCATION("CELLULAR_LOCATION___DNA___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___UNDEFINED___CELLULAR_LOCATION("CELLULAR_LOCATION___UNDEFINED___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___REACTION___CELLULAR_LOCATION("CELLULAR_LOCATION___REACTION___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___REGULATION___CELLULAR_LOCATION("CELLULAR_LOCATION___REGULATION___CELLULAR_LOCATION"),
        CELLULAR_LOCATION___CATALYSIS___CELLULAR_LOCATION("CELLULAR_LOCATION___CATALYSIS___CELLULAR_LOCATION"),

        REACTANT___REACTION___SMALL_MOLECULE("REACTANT___REACTION___SMALL_MOLECULE"),
        REACTANT___REACTION___PROTEIN("REACTANT___REACTION___PROTEIN"),
        REACTANT___REACTION___PHYSICAL_ENTITY_COMPLEX("REACTANT___REACTION___PHYSICAL_ENTITY_COMPLEX"),
        REACTANT___REACTION___UNDEFINED("REACTANT___REACTION___UNDEFINED"),
        REACTANT___REACTION___DNA("REACTANT___REACTION___DNA"),
        REACTANT___REACTION___RNA("REACTANT___REACTION___RNA"),

        PRODUCT___REACTION___PROTEIN("PRODUCT___REACTION___PROTEIN"),
        PRODUCT___REACTION___SMALL_MOLECULE("PRODUCT___REACTION___SMALL_MOLECULE"),
        PRODUCT___REACTION___PHYSICAL_ENTITY_COMPLEX("PRODUCT___REACTION___PHYSICAL_ENTITY_COMPLEX"),
        PRODUCT___REACTION___UNDEFINED("PRODUCT___REACTION___UNDEFINED"),
        PRODUCT___REACTION___RNA("PRODUCT___REACTION___RNA"),

        CONTROLLER___CATALYSIS___PROTEIN("CONTROLLER___CATALYSIS___PROTEIN"),
        CONTROLLER___CATALYSIS___PHYSICAL_ENTITY_COMPLEX("CONTROLLER___CATALYSIS___PHYSICAL_ENTITY_COMPLEX"),
        CONTROLLER___CATALYSIS___UNDEFINED("CONTROLLER___CATALYSIS___UNDEFINED"),
        CONTROLLER___REGULATION___PHYSICAL_ENTITY_COMPLEX("CONTROLLER___REGULATION___PHYSICAL_ENTITY_COMPLEX"),
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

        ANNOTATION___GENE___XREF("ANNOTATION___GENE___XREF"),
        ANNOTATION___PROTEIN___XREF("ANNOTATION___PROTEIN___XREF"),
        ANNOTATION___TRANSCRIPT___XREF("ANNOTATION___TRANSCRIPT___XREF"),
        XREF___RNA___XREF("XREF___RNA___XREF"),

        IS___DNA___GENE("IS___DNA___GENE"),
        IS___RNA___MIRNA("IS___RNA___MIRNA"),

        IS___GENE___MIRNA("IS___GENE___MIRNA"),
        TARGET___GENE___MIRNA_MATURE("TARGET___GENE___MIRNA_MATURE"),
        MATURE___MIRNA___MIRNA_MATURE("MATURE___MIRNA___MIRNA_MATURE");

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
        //this.bioPAXImporter = new Neo4jBioPaxBuilder()

        this.inputPath = inputPath;
        this.outputPath = outputPath;

//        sampleIds = new ArrayList<>();

        csvWriters = new HashMap<>();
        csvAnnotatedWriters = new HashMap<>();

        rocksDbManager = new RocksDbManager();
        uidRocksDb = this.rocksDbManager.getDBConnection(outputPath.toString() + "/uidRocksDB", true);

        geneCache = new GeneCache(outputPath);
        proteinCache = new ProteinCache(outputPath);

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

    public void openCSVFiles(List<File> variantFiles) throws IOException {
        PrintWriter pw;
        String filename;

        noAttributes = createNoAttributes();
        nodeAttributes = createNodeAttributes(variantFiles);

        // CSV files for nodes
        for (Node.Type type : Node.Type.values()) {
            filename = type.toString() + ".csv";

            pw = new PrintWriter(outputPath + "/" + filename);
            csvWriters.put(type.toString(), pw);

            if (CollectionUtils.isNotEmpty(nodeAttributes.get(type.toString()))) {
                pw.println(getNodeHeaderLine(nodeAttributes.get(type.toString())));
            }
        }

        // CSV files for relationships
        for (Relation.Type type : Relation.Type.values()) {
            if (type.toString().contains("__")) {
                filename = type.toString() + ".csv";
//                pw = new PrintWriter(new BufferedWriter(new FileWriter(outputPath + "/" + filename, !header)));
                pw = new PrintWriter(outputPath + "/" + filename);
                csvWriters.put(type.toString(), pw);

//                if (type != Relation.Type.VARIANT__VARIANT_CALL
//                        && type != Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO
//                        && type != Relation.Type.VARIANT_FILE_INFO__FILE
//                        && type != Relation.Type.SAMPLE__VARIANT_CALL) {
                pw.println(getRelationHeaderLine(type.toString()));
//                }
            }
        }

        // CSV files for BioPAX relationships
        for (BioPAXRelation type : BioPAXRelation.values()) {
            filename = type.toString() + ".csv";
            pw = new PrintWriter(outputPath + "/" + filename);
            csvWriters.put(type.toString(), pw);

            pw.println(getBioPAXRelationHeaderLine(type.toString()));
        }
    }

    public void close() {
        List<Map<String, PrintWriter>> writerMaps = new ArrayList<>();
        writerMaps.add(csvWriters);
        writerMaps.add(csvAnnotatedWriters);

        for (Map<String, PrintWriter> writerMap : writerMaps) {
            if (MapUtils.isNotEmpty(writerMap)) {
                Iterator<PrintWriter> iterator = writerMap.values().iterator();
                while (iterator.hasNext()) {
                    iterator.next().close();
                }
            }
        }
    }

//    public void openMetadataFile(File metafile) throws IOException {
//        BufferedReader bufferedReader = FileUtils.newBufferedReader(metafile.toPath());
//        String metadata = bufferedReader.readLine();
//        bufferedReader.close();
//
//        //FileUtils.readFully(new BufferedReader(new FileReader(metafile.getAbsolutePath())));
////        String metadata = FileUtils.readFully(new BufferedReader(new FileReader(metafile.getAbsolutePath())));
//        ObjectMapper mapper = new ObjectMapper();
//        VariantMetadata variantMetadata = mapper.readValue(metadata, VariantMetadata.class);
//        if (CollectionUtils.isNotEmpty(variantMetadata.getStudies())) {
//            VariantStudyMetadata variantStudyMetadata = variantMetadata.getStudies().get(0);
//            for (Individual individual: variantStudyMetadata.getIndividuals()) {
//                sampleNames.add(individual.getSamples().get(0).getId());
//            }
//
//            if (CollectionUtils.isNotEmpty(variantStudyMetadata.getFiles())) {
//                for (VariantFileMetadata variantFileMetadata: variantStudyMetadata.getFiles()) {
//                    if (StringUtils.isNotEmpty(variantFileMetadata.getId())) {
//                        Long fileUid = getLong(variantFileMetadata.getId(), Node.Type.VARIANT_FILE.toString());
//                        if (fileUid == null) {
//                            // File node
//                            Node n = new Node(getAndIncUid(), variantFileMetadata.getId(), variantFileMetadata.getPath(),
//                                    Node.Type.VARIANT_FILE);
//                            csvWriters.get(Node.Type.VARIANT_FILE.toString()).println(nodeLine(n));
//                            putLong(variantFileMetadata.getId(), Node.Type.VARIANT_FILE.toString(), n.getUid());
//                        }
//                    }
//
//                    if (variantFileMetadata.getHeader() != null) {
//                        for (VariantFileHeaderComplexLine line: variantFileMetadata.getHeader().getComplexLines()) {
//                            if ("INFO".equals(line.getKey())) {
//                                infoFields.add(line.getId());
//                            } else if ("FORMAT".equals(line.getKey())) {
//                                formatFields.add(line.getId());
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        // Variant call
//        String strType;
//        List<String> attrs = new ArrayList<>();
//        attrs.add("variantCallId");
//        Iterator<String> iterator = formatFields.iterator();
//        while (iterator.hasNext()) {
//            attrs.add(iterator.next());
//        }
//        strType = Node.Type.VARIANT_SAMPLE_FORMAT.toString();
//        nodeAttributes.put(strType, attrs);
//        csvWriters.get(strType).println(getNodeHeaderLine(attrs));
//        strType = Relation.Type.VARIANT__VARIANT_CALL.toString();
//        csvWriters.get(strType).println(getRelationHeaderLine(strType));
//        strType = Relation.Type.SAMPLE__VARIANT_CALL.toString();
//        csvWriters.get(strType).println(getRelationHeaderLine(strType));
//
//        // Variant file info
//        attrs = new ArrayList<>();
//        attrs.add("variantFileInfoId");
//        iterator = infoFields.iterator();
//        while (iterator.hasNext()) {
//            attrs.add(iterator.next());
//        }
//        strType = Node.Type.VARIANT_FILE_INFO.toString();
//        nodeAttributes.put(strType, attrs);
//        csvWriters.get(strType).println(getNodeHeaderLine(attrs));
//        strType = Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString();
//        csvWriters.get(strType).println(getRelationHeaderLine(strType));
//        strType = Relation.Type.VARIANT_FILE_INFO__FILE.toString();
//        csvWriters.get(strType).println(getRelationHeaderLine(strType));
//    }

    public Long getGeneUid(String xrefId) {
        Long geneUid = null;
        String geneId = geneCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(geneId)) {
            geneUid = getLong(geneId, Node.Type.GENE.name());
        } else {
            logger.info("Getting gene UID: Xref not found for gene {}", xrefId);
        }
        return geneUid;
    }

    public void saveGeneUid(String xrefId, Long geneUid) {
        String geneId = geneCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(geneId)) {
            putLong(geneId, Node.Type.GENE.name(), geneUid);
        } else {
            logger.info("Setting gene UID {}: Xref not found for gene {}", geneUid, xrefId);
        }
    }

    public void saveUnknownGeneUid(String geneId, String geneName, Long geneUid) {
        // Add prefix "g." to avoid duplicated names (i.e., proteins and genes have the same name)
        geneCache.addXrefId(geneId, "g." + geneId);
        if (StringUtils.isNotEmpty(geneName)) {
            geneCache.addXrefId(geneName, "g." + geneId);
        }

        putLong(geneId, Node.Type.GENE.name(), geneUid);
    }

    public Gene getGene(String xrefId) {
        return geneCache.get(xrefId);
    }

    public Long getProteinUid(String xrefId) {
        Long proteinUid = null;
        String proteinId = proteinCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(proteinId)) {
            proteinUid = getLong(proteinId, Node.Type.PROTEIN.name());
        } else {
            logger.info("Getting protein UID: Xref not found for protein {}", xrefId);
        }
        return proteinUid;
    }

    public void saveProteinUid(String xrefId, Long proteinUid) {
        String proteinId = proteinCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(proteinId)) {
            putLong(proteinId, Node.Type.PROTEIN.name(), proteinUid);
        } else {
            logger.info("Setting protein UID {}: Xref not found for protein {}", proteinUid, xrefId);
        }
    }

    public void saveUnknownProteinUid(String proteinId, String proteinName, Long proteinUid) {
        proteinCache.addXrefId(proteinId, proteinId);
        if (StringUtils.isNotEmpty(proteinName)) {
            proteinCache.addXrefId(proteinName, proteinId);
        }

        putLong(proteinId, Node.Type.PROTEIN.name(), proteinUid);
    }

    public Entry getProtein(String xrefId) {
        return proteinCache.get(xrefId);
    }

    public String getMiRnaInfo(String id) {
        String info = rocksDbManager.getString(id, miRnaRocksDb);
        if (StringUtils.isNotEmpty(info)) {
            return id + ":" + info;
        }

        return info;
    }

    public Long getLong(String id, String type) {
        return rocksDbManager.getLong(id + "." + type, uidRocksDb);
    }

//    public Long getLong(String key) {
//        return rocksDbManager.getLong(key, uidRocksDb);
//    }

//    public String getString(String key) {
//        return rocksDbManager.getString(key, uidRocksDb);
//    }

    public void putLong(String id, String type, long value) {
        rocksDbManager.putLong(id + "." + type, value, uidRocksDb);
    }

//    public void putLong(String key, long value) {
//        rocksDbManager.putLong(key, value, uidRocksDb);
//    }

//    public void putString(String key, String value) {
//        rocksDbManager.putString(key, value, uidRocksDb);
//    }

    protected String getNodeHeaderLine(List<String> attrs) {
        StringBuilder sb = new StringBuilder();
        sb.append("uid:ID(").append(attrs.get(0)).append(")");
        for (int i = 1; i < attrs.size(); i++) {
            sb.append(SEPARATOR);
            if (!noAttributes.contains(attrs.get(i))) {
                sb.append(PREFIX_ATTRIBUTES);
            }
            sb.append(attrs.get(i));
        }
        sb.append(SEPARATOR).append(":LABEL");
        return sb.toString();
    }

    protected String getRelationHeaderLine(String type) {
        StringBuilder sb = new StringBuilder();

        String source;
        String dest;
        if (type.contains("___")) {
            String[] split = type.split("___");
            source = split[1];
            dest = split[2];
        } else {
            String[] split = type.split("__");
            source = split[0];
            dest = split[1];
        }

        if (CollectionUtils.isNotEmpty(nodeAttributes.get(source)) && CollectionUtils.isNotEmpty(nodeAttributes.get(dest))) {
            sb.append(":START_ID(").append(nodeAttributes.get(source).get(0)).append(")").append(SEPARATOR).append(":END_ID(")
                    .append(nodeAttributes.get(dest).get(0)).append(")");
        } else {
            if (CollectionUtils.isEmpty(nodeAttributes.get(source))) {
                logger.info("Attributes empty for " + source + ", from getRelationHeaderLine: " + type);
            }
            if (CollectionUtils.isEmpty(nodeAttributes.get(dest))) {
                logger.info("Attributes empty for " + dest + ", from getRelationHeaderLine: " + type);
            }
        }
        return sb.toString();
    }

    private String getBioPAXRelationHeaderLine(String type) {
        StringBuilder sb = new StringBuilder();
        String[] split = type.split("___");
        sb.append(":START_ID(").append(nodeAttributes.get(split[1]).get(0)).append(")").append(SEPARATOR).append(":END_ID(")
                .append(nodeAttributes.get(split[2]).get(0)).append(")");
        if (type.equals(BioPAXRelation.TARGET___GENE___MIRNA_MATURE.toString())) {
            sb.append(CsvInfo.SEPARATOR + PREFIX_ATTRIBUTES + "experiment");
            sb.append(CsvInfo.SEPARATOR + PREFIX_ATTRIBUTES + "evidence");
            sb.append(CsvInfo.SEPARATOR + PREFIX_ATTRIBUTES + "pubmed");
        }
        return sb.toString();
    }

    // Debug purposes
    private Set<String> notdefined = new HashSet<>();

    public String nodeLine(Node node) {
        List<String> attrs = nodeAttributes.get(node.getType().toString());
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isEmpty(attrs)) {
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

        // Labels
        switch (node.getType()) {
            case GENE: {
                if (node.getAttributes().containsKey("source")) {
                    if (node.getAttributes().getString("source").equals("ensembl")) {
                        sb.append(labelsLine(ENSEMBL_GENE));
                    } else if (node.getAttributes().getString("source").equals("refseq")) {
                        sb.append(labelsLine(REFSEQ_GENE));
                    } else {
                        sb.append(labelsLine(GENE));
                    }
                } else if (node.getId().startsWith("ENSG")) {
                    sb.append(labelsLine(ENSEMBL_GENE));
                } else {
                    sb.append(labelsLine(GENE));
                }
                break;
            }
            case TRANSCRIPT: {
                if (node.getAttributes().containsKey("source")) {
                    if (node.getAttributes().getString("source").equals("ensembl")) {
                        sb.append(labelsLine(ENSEMBL_TRANSCRIPT));
                    } else if (node.getAttributes().getString("source").equals("refseq")) {
                        sb.append(labelsLine(REFSEQ_TRANSCRIPT));
                    } else {
                        sb.append(labelsLine(TRANSCRIPT));
                    }
                } else if (node.getId().startsWith("ENST")) {
                    sb.append(labelsLine(ENSEMBL_TRANSCRIPT));
                } else {
                    sb.append(labelsLine(TRANSCRIPT));
                }
                break;
            }
            case EXON: {
                if (node.getAttributes().containsKey("source")) {
                    if (node.getAttributes().getString("source").equals("ensembl")) {
                        sb.append(labelsLine(ENSEMBL_EXON));
                    } else if (node.getAttributes().getString("source").equals("refseq")) {
                        sb.append(labelsLine(REFSEQ_EXON));
                    } else {
                        sb.append(labelsLine(EXON));
                    }
                } else if (node.getId().startsWith("ENSE")) {
                    sb.append(labelsLine(ENSEMBL_EXON));
                } else {
                    sb.append(labelsLine(EXON));
                }
                break;
            }
            default:
                sb.append(labelsLine(node.getType()));
                break;
        }
        return sb.toString();
    }

    private String labelsLine(Node.Type type) {
        StringBuilder sb = new StringBuilder();
        sb.append(SEPARATOR).append(type.getLabels().get(0));
        for (int i = 1; i < type.getLabels().size(); i++) {
            sb.append(ARRAY_SEPARATOR).append(type.getLabels().get(i));
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
            return input.replace("\"", "");
        } else {
            return null;
        }
    }

//    public void buildGenes(Path inputPath) throws IOException {
////        geneCache.index(inputPath, indexPath);
//
////        String objFilename = output.toString() + "/genes.rocksdb";
////        String xrefObjFilename = output.toString() + "/xref.genes.rocksdb";
//
////        if (Paths.get(objFilename).toFile().exists()
////                && Paths.get(xrefObjFilename).toFile().exists()) {
////            objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
////            xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);
////            logger.info("\tGene index already created!");
////            return;
////        }
//
////        // Delete protein RocksDB files
////        Paths.get(objFilename).toFile().delete();
////        Paths.get(xrefObjFilename).toFile().delete();
////
////        // Create gene RocksDB files (protein and xrefs)
////        RocksDB objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
////        RocksDB xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);
//
//        BufferedReader reader = org.opencb.commons.utils.FileUtils.newBufferedReader(inputPath);
//        String jsonGene = reader.readLine();
//        long geneCounter = 0;
//        while (jsonGene != null) {
//            Gene gene = geneCache.getObjReader().readValue(jsonGene);
//            String geneId = gene.getId();
//            if (org.apache.commons.lang3.StringUtils.isNotEmpty(geneId)) {
//                geneCounter++;
//                if (geneCounter % 5000 == 0) {
//                    logger.info("Indexing {} genes...", geneCounter);
//                }
//                // Save gene
//                rocksDbManager.putString(geneId, jsonGene, geneCache.getObjRocksDb());
//
//                // Save xrefs for that gene
//                rocksDbManager.putString(geneId, geneId, geneCache.getXrefObjRocksDb());
//                if (org.apache.commons.lang3.StringUtils.isNotEmpty(gene.getName())) {
//                    rocksDbManager.putString(gene.getName(), geneId, geneCache.getXrefObjRocksDb());
//                }
//
//                if (ListUtils.isNotEmpty(gene.getTranscripts())) {
//                    for (Transcript transcr : gene.getTranscripts()) {
//                        if (ListUtils.isNotEmpty(transcr.getXrefs())) {
//                            for (Xref xref: transcr.getXrefs()) {
//                                if (org.apache.commons.lang3.StringUtils.isNotEmpty(xref.getId())) {
//                                    rocksDbManager.putString(xref.getId(), geneId, geneCache.getXrefObjRocksDb());
//                                }
//                            }
//                        }
//                    }
//                }
//            } else {
//                logger.info("Skipping indexing gene: missing gene ID from JSON file");
//            }
//
//            // Next line
//            jsonGene = reader.readLine();
//        }
//        logger.info("Indexing {} genes. Done.", geneCounter);
//
//        reader.close();
//    }

//    public void buildProteins(Path inputPath, Path indexPath) throws IOException {
//        proteinCache.index(inputPath, indexPath);
//    }

    public void indexingMiRnas(Path miRnaPath, Path indexPath) throws IOException {
        RocksDbManager rocksDbManager = new RocksDbManager();

        if (indexPath.toFile().exists()) {
            logger.info("\tmiRNA index already created!");
            miRnaRocksDb = rocksDbManager.getDBConnection(indexPath.toString(), false);
            return;
        }
        miRnaRocksDb = rocksDbManager.getDBConnection(indexPath.toString(), false);

        BufferedReader reader = FileUtils.newBufferedReader(miRnaPath);
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
                String info = rocksDbManager.getString(miRna, miRnaRocksDb);
                if (info == null) {
                    info = targetGene + ":" + evidence;
                } else {
                    info = info + "::" + targetGene + ":" + evidence;
                }
                rocksDbManager.putString(miRna, info, miRnaRocksDb);
            }

            // Next line
            line = reader.readLine();
        }
        logger.info("Indexing {} miRNAs. Done.", miRnaCounter);

        reader.close();
    }

    private Map<String, List<String>> createNodeAttributes(List<File> variantFiles) {
        List<String> attrs;
        Map<String, List<String>> nodeAttributes = new HashMap<>();

        //
        // Variant related-nodes
        //

        // Variant
        attrs = Arrays.asList("variantId", "id", "name", "alternativeNames", "chromosome", "start", "end", "strand",
                "reference", "alternate", "type");
        nodeAttributes.put(Node.Type.VARIANT.toString(), new ArrayList<>(attrs));

        // Population frequency
        attrs = Arrays.asList("variantPopulationFrequencyId", "id", "name", "study", "population", "refAlleleFreq", "altAlleleFreq");
        nodeAttributes.put(Node.Type.VARIANT_POPULATION_FREQUENCY.toString(), new ArrayList<>(attrs));

        // Conservation
        attrs = Arrays.asList("variantConservationScoreId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Type.VARIANT_CONSERVATION_SCORE.toString(), new ArrayList<>(attrs));

        // HGV
        attrs = Arrays.asList("hgvId", "id", "name");
        nodeAttributes.put(Node.Type.HGV.toString(), new ArrayList<>(attrs));

        // Genomic feature
        attrs = Arrays.asList("genomicFeatureId", "id", "name", "type", "geneName", "transcriptId");
        nodeAttributes.put(Node.Type.GENOMIC_FEATURE.toString(), new ArrayList<>(attrs));

        // Functional score
        attrs = Arrays.asList("variantFunctionalScoreId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Type.VARIANT_FUNCTIONAL_SCORE.toString(), new ArrayList<>(attrs));

        // Variant drug interaction
        nodeAttributes.put(Node.Type.VARIANT_DRUG_INTERACTION.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("variantDrugInteractionId", "id", "name", "therapeuticContext", "pathway", "effect", "association", "status",
                "evidence", "bibliography");

        // Cytoband
        nodeAttributes.put(Node.Type.CYTOBAND.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("cytobandId", "id", "name", "chromosome", "start", "end", "stain");

        // Repeat
        nodeAttributes.put(Node.Type.REPEAT.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("repeatId", "id", "name", "chromosome", "start", "end", "period", "consensusSize", "copyNumber",
                "percentageMatch", "score", "source");

        // Structural variation
        nodeAttributes.put(Node.Type.STRUCTURAL_VARIATION.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("structuralVariationId", "id", "name", "ciStartLeft", "ciStartRight", "ciEndLeft", "ciEndRight", "copyNumber",
                "leftSvInSeq", "rightSvInSeq",
                "type");

        // Breakend
        nodeAttributes.put(Node.Type.BREAKEND.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("breakendId", "id", "name", "orientation");

        // Breakend mate
        nodeAttributes.put(Node.Type.BREAKEND_MATE.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("breakendMateId", "id", "name", "chromosome", "position", "ciPositionLeft", "ciPositionRight");

        // Clinical evidence
        nodeAttributes.put(Node.Type.CLINICAL_EVIDENCE.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("clinicalEvidenceId", "id", "name", "url", "sourceName", "sourceVersion", "sourceDate", "alleleOrigin",
                "primarySite", "siteSubtype", "primaryHistology", "histologySubtype", "tumorOrigin", "sampleSource", "impact", "confidence",
                "consistencyStatus", "ethnicity", "penetrance", "variableExpressivity", "description", "bibliography");

        // Evidence submission
        nodeAttributes.put(Node.Type.EVIDENCE_SUBMISSION.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("evidenceSubmissionId", "id", "name", "submitter", "date");

        // Heritable trait
        nodeAttributes.put(Node.Type.HERITABLE_TRAIT.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("heritableTraitId", "id", "name", "trait", "inheritanceMode");

        // Property
        nodeAttributes.put(Node.Type.PROPERTY.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("propertyId", "id", "name", "value");

        // Variant classification
        attrs = Arrays.asList("variantClassificationId", "id", "name", "acmg", "clinicalSignificance", "drugResponse", "traitAssociation",
                "functionalEffect", "tumorigenesis");
        nodeAttributes.put(Node.Type.VARIANT_CLASSIFICATION.toString(), new ArrayList<>(attrs));

        // Consequence type
        attrs = Arrays.asList("variantConsequenceTypeId", "id", "name", "study", "biotype", "cdnaPosition", "cdsPosition", "codon",
                "strand", "gene", "transcript", "transcriptAnnotationFlags", "exonOverlap");
        nodeAttributes.put(Node.Type.VARIANT_CONSEQUENCE_TYPE.toString(), new ArrayList<>(attrs));

        // Protein variant annotation
        attrs = Arrays.asList("proteinVariantAnnotationId", "id", "name", "position", "reference", "alternate",
                "functionalDescription");
        nodeAttributes.put(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        // File
        attrs = Arrays.asList("fileId", "id", "name");
        nodeAttributes.put(Node.Type.VARIANT_FILE.toString(), new ArrayList<>(attrs));

        // Family
        attrs = Arrays.asList("familyId", "id", "name");
        nodeAttributes.put(Node.Type.FAMILY.toString(), new ArrayList<>(attrs));

        // Individual
        attrs = Arrays.asList("individualId", "id", "name", "sex", "phenotype");
        nodeAttributes.put(Node.Type.INDIVIDUAL.toString(), new ArrayList<>(attrs));

        // Sample, variant file info and variant sample format
        nodeAttributes.putAll(createSampleRelatedAttrs(variantFiles));

        //
        // Gene related-nodes
        //

        // Gene
        attrs = Arrays.asList("geneId", "id", "name", "biotype", "chromosome", "start", "end", "strand", "description",
                "version", "source", "status");
        nodeAttributes.put(Node.Type.GENE.toString(), new ArrayList<>(attrs));

        // Disease panel
        attrs = Arrays.asList("diseasePanelId", "id", "name", "description", "phenotypeNames", "sourceId", "sourceName",
                "sourceAuthor", "sourceProject", "sourceVersion", "creationDate", "modificationDate");
        nodeAttributes.put(Node.Type.DISEASE_PANEL.toString(), new ArrayList<>(attrs));

        // Panel gene
        attrs = Arrays.asList("panelGeneId", "id", "name", "modeOfInheritance", "penetrance", "confidence", "evidences", "publications",
                "coordinates");
        nodeAttributes.put(Node.Type.PANEL_GENE.toString(), new ArrayList<>(attrs));

        // Gene drug interaction
        attrs = Arrays.asList("geneDrugInteractionId", "id", "name", "source", "type", "studyType", "chemblId");
        nodeAttributes.put(Node.Type.GENE_DRUG_INTERACTION.toString(), new ArrayList<>(attrs));

        // Drug
        attrs = Arrays.asList("drugId", "id", "name");
        nodeAttributes.put(Node.Type.DRUG.toString(), new ArrayList<>(attrs));

        // Gene trait association
        attrs = Arrays.asList("geneTraitAssociationId", "id", "name", "hpo", "numberOfPubmeds", "score", "source", "sources",
                "associationType");
        nodeAttributes.put(Node.Type.GENE_TRAIT_ASSOCIATION.toString(), new ArrayList<>(attrs));

        // Gene expression
        attrs = Arrays.asList("geneExpressionId", "id", "name", "transcriptId", "experimentalFactor", "factorValue", "experimentId",
                "technologyPlatform", "expressionCall", "pValue");
        nodeAttributes.put(Node.Type.GENE_EXPRESSION.toString(), new ArrayList<>(attrs));

        // Transcript
        attrs = Arrays.asList("transcriptId", "id", "name", "chromosome", "start", "end", "strand", "biotype", "status",
                "genomicCodingStart", "genomicCodingEnd", "cdnaCodingStart", "cdnaCodingEnd", "cdsLength", "description", "version",
                "source", "annotationFlags");
        nodeAttributes.put(Node.Type.TRANSCRIPT.toString(), new ArrayList<>(attrs));

        // Exon
        attrs = Arrays.asList("exonId", "id", "name", "chromosome", "start", "end", "strand", "genomicCodingStart",
                "genomicCodingEnd", "cdnaCodingStart", "cdnaCodingEnd", "cdsStart", "cdsEnd", "phase", "exonNumber", "source");
        nodeAttributes.put(Node.Type.EXON.toString(), new ArrayList<>(attrs));

        // Constraint
        attrs = Arrays.asList("transcriptConstraintScoreId", "id", "name", "source", "method", "value");
        nodeAttributes.put(Node.Type.TRANSCRIPT_CONSTRAINT_SCORE.toString(), new ArrayList<>(attrs));

        // Feature ontology term annotation
        attrs = Arrays.asList("featureOntologyTermAnnotationId", "id", "name", "source", "attributes");
        nodeAttributes.put(Node.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION.toString(), new ArrayList<>(attrs));

        // Annotation evidence
        attrs = Arrays.asList("transcriptAnnotationEvidenceId", "id", "name", "references", "qualifier");
        nodeAttributes.put(Node.Type.TRANSCRIPT_ANNOTATION_EVIDENCE.toString(), new ArrayList<>(attrs));

        //        // Target transcript
//        attrs = Arrays.asList("rnaId", "id", "name", "evidence");
//        nodeAttributes.put(Node.Type.TARGET_TRANSCRIPT.toString(), new ArrayList<>(attrs));

        // miRNA
        attrs = Arrays.asList("miRnaId", "id", "name", "accession", "status", "sequence");
        nodeAttributes.put(Node.Type.MIRNA.toString(), new ArrayList<>(attrs));

        // miRNA mature
        attrs = Arrays.asList("miRnaMatureId", "id", "name", "accession", "sequence", "start", "end");
        nodeAttributes.put(Node.Type.MIRNA_MATURE.toString(), new ArrayList<>(attrs));

        // miRNA target
        attrs = Arrays.asList("miRnaTargetId", "id", "name", "experiment", "evidence", "pubmed");
        nodeAttributes.put(Node.Type.MIRNA_TARGET.toString(), new ArrayList<>(attrs));

        // TFBS
        attrs = Arrays.asList("tfbsId", "id", "name", "chromosome", "start", "end", "strand", "relativeStart",
                "relativeEnd", "score", "pwm");
        nodeAttributes.put(Node.Type.TFBS.toString(), new ArrayList<>(attrs));

        // Xref
        attrs = Arrays.asList("xrefId", "id", "name", "dbName", "dbDisplayName", "description");
        nodeAttributes.put(Node.Type.XREF.toString(), new ArrayList<>(attrs));

        // SO_TERM
        attrs = Arrays.asList("soTermId", "id", "name");
        nodeAttributes.put(Node.Type.SO_TERM.toString(), new ArrayList<>(attrs));

        //
        // Protein related-nodes
        //

        // Protein
        attrs = Arrays.asList("protId", "id", "name", "accession", "dataset", "proteinExistence", "evidence", "object");
        nodeAttributes.put(Node.Type.PROTEIN.toString(), new ArrayList<>(attrs));

        // Protein object  (object = gz json)
        attrs = Arrays.asList("proteinObjectId", "id", "name", "object");
        nodeAttributes.put(Node.Type.PROTEIN_OBJECT.toString(), new ArrayList<>(attrs));

        // Protein keyword
        attrs = Arrays.asList("proteinKeywordId", "id", "name", "evidence");
        nodeAttributes.put(Node.Type.PROTEIN_KEYWORD.toString(), new ArrayList<>(attrs));

        // Protein feature
        attrs = Arrays.asList("proteinFeatureId", "id", "name", "type", "evidence", "locationPosition", "locationBegin",
                "locationEnd", "description");
        nodeAttributes.put(Node.Type.PROTEIN_FEATURE.toString(), new ArrayList<>(attrs));

        // Protein variant annotation
        attrs = Arrays.asList("proteinVariantAnnotationId", "id", "name");
        nodeAttributes.put(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        // Substitution score
        attrs = Arrays.asList("proteinSubstitutionScoreId", "id", "name", "score");
        nodeAttributes.put(Node.Type.PROTEIN_SUBSTITUTION_SCORE.toString(), new ArrayList<>(attrs));

        //
        // Pathway related-nodes
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
        attrs = Arrays.asList("physicalEntityComplexId", "id", "name");
        nodeAttributes.put(Node.Type.PHYSICAL_ENTITY_COMPLEX.toString(), new ArrayList<>(attrs));

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

        //
        // Clinical related-nodes
        //

//        // Clinical Analysis
//        attrs = Arrays.asList("clinicalAnalysisId", "id", "name", "uuid", "description", "type", "priority", "flags", "creationDate",
//                "modificationDate", "dueDate", "statusName", "statusDate", "statusMessage", "consentPrimaryFindings",
//                "consentSecondaryFindings", "consentCarrierFindings", "consentResearchFindings", "release");
//        nodeAttributes.put(Node.Type.CLINICAL_ANALYSIS.toString(), new ArrayList<>(attrs));
//
//        // Clinical analyst
//        attrs = Arrays.asList("clinicalAnalystId", "id", "name", "assignedBy", "assignee", "date");
//        nodeAttributes.put(Node.Type.CLINICAL_ANALYST.toString(), new ArrayList<>(attrs));
//
//        // Comment
//        attrs = Arrays.asList("commentId", "id", "name", "author", "type", "text", "date");
//        nodeAttributes.put(Node.Type.COMMENT.toString(), new ArrayList<>(attrs));
//
//        // Interpretation
//        attrs = Arrays.asList("interpretationId", "id", "name", "uuid", "description", "status", "creationDate", "version");
//        nodeAttributes.put(Node.Type.INTERPRETATION.toString(), new ArrayList<>(attrs));
//
//        // Software
//        attrs = Arrays.asList("softwareId", "id", "name", "version", "repository", "commit", "website", "params");
//        nodeAttributes.put(Node.Type.SOFTWARE.toString(), new ArrayList<>(attrs));
//
//        // Reported variant
//        attrs = Arrays.asList("reportedVariantId", "id", "name", "deNovoQualityScore", "status", "attributes");
//        nodeAttributes.put(Node.Type.REPORTED_VARIANT.toString(), new ArrayList<>(attrs));
//
//        // Low covarage
//        attrs = Arrays.asList("lowCoverageId", "id", "name", "geneName", "chromosome", "start", "end", "meanCoverage", "type");
//        nodeAttributes.put(Node.Type.LOW_COVERAGE_REGION.toString(), new ArrayList<>(attrs));
//
//        // Analyst
//        attrs = Arrays.asList("analystId", "id", "name", "company", "email");
//        nodeAttributes.put(Node.Type.ANALYST.toString(), new ArrayList<>(attrs));
//
//        // Reported event
//        attrs = Arrays.asList("reportedVariantId", "id", "name", "modeOfInheritance", "penetrance", "score", "fullyExplainPhenotypes",
//                "roleInCancer", "actionable", "justification", "tier");
//        nodeAttributes.put(Node.Type.REPORTED_EVENT.toString(), new ArrayList<>(attrs));

        return nodeAttributes;
    }


    private Map<String, List<String>> createSampleRelatedAttrs(List<File> variantFiles) {
        List<String> attrs;
        Map<String, List<String>> nodeAttributes = new HashMap<>();

        // For variant file info, variant sample format and sample nodes we have to read variant metadata files to know which attributes
        // are present
        Set<String> sampleAttrs = new HashSet<>();
        Set<String> formatAttrs = new HashSet<>();
        Set<String> infoAttrs = new HashSet<>();

        if (CollectionUtils.isNotEmpty(variantFiles)) {
            for (File variantFile : variantFiles) {
                File metaFile = new File(variantFile.getAbsoluteFile() + ".meta.json");
                if (!metaFile.exists()) {
                    metaFile = new File(variantFile.getAbsoluteFile() + ".meta.json.gz");
                }
                if (!metaFile.exists()) {
                    continue;
                }

                // Read info, format and sample from metadata file
                ObjectMapper mapper = new ObjectMapper();
                VariantMetadata variantMetadata;
                try {
                    BufferedReader bufferedReader = FileUtils.newBufferedReader(metaFile.toPath());
                    String metadata = bufferedReader.readLine();
                    bufferedReader.close();
                    variantMetadata = mapper.readValue(metadata, VariantMetadata.class);
                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

                if (CollectionUtils.isNotEmpty(variantMetadata.getStudies())) {
                    // IMPORTANT: it considers only the first study
                    VariantStudyMetadata variantStudyMetadata = variantMetadata.getStudies().get(0);

                    // Get sample attributes
                    for (Individual individual : variantStudyMetadata.getIndividuals()) {
                        if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                            for (Sample sample : individual.getSamples()) {
                                if (MapUtils.isNotEmpty(sample.getAnnotations())) {
                                    sampleAttrs.addAll(sample.getAnnotations().keySet());
                                }
                            }
                        }
                    }

                    if (CollectionUtils.isNotEmpty(variantStudyMetadata.getFiles())) {
                        for (VariantFileMetadata variantFileMetadata : variantStudyMetadata.getFiles()) {
//                            if (StringUtils.isNotEmpty(variantFileMetadata.getId())) {
//                                Long fileUid = getLong(variantFileMetadata.getId(), Node.Type.VARIANT_FILE.toString());
//                                if (fileUid == null) {
//                                    // File node
//                                    Node n = new Node(getAndIncUid(), variantFileMetadata.getId(), variantFileMetadata.getPath(),
//                                            Node.Type.VARIANT_FILE);
//                                    csvWriters.get(Node.Type.VARIANT_FILE.toString()).println(nodeLine(n));
//                                    putLong(variantFileMetadata.getId(), Node.Type.VARIANT_FILE.toString(), n.getUid());
//                                }
//                            }

                            if (variantFileMetadata.getHeader() != null) {
                                for (VariantFileHeaderComplexLine line : variantFileMetadata.getHeader().getComplexLines()) {
                                    if ("INFO".equals(line.getKey())) {
                                        infoAttrs.add(line.getId());
                                    } else if ("FORMAT".equals(line.getKey())) {
                                        formatAttrs.add(line.getId());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sample
        attrs = new ArrayList<>();
        attrs.addAll(Arrays.asList("sampleId", "id", "name"));
        if (CollectionUtils.isNotEmpty(sampleAttrs)) {
            CollectionUtils.addAll(attrs, sampleAttrs.iterator());
        }
        nodeAttributes.put(Node.Type.SAMPLE.toString(), attrs);

        // Variant file info
        attrs = new ArrayList<>();
        attrs.addAll(Arrays.asList("variantFileDataId", "id", "name"));
        if (CollectionUtils.isNotEmpty(infoAttrs)) {
            CollectionUtils.addAll(attrs, infoAttrs.iterator());
        }
        nodeAttributes.put(Node.Type.VARIANT_FILE_DATA.toString(), attrs);

        // Variant sample format
        attrs = new ArrayList<>();
        attrs.addAll(Arrays.asList("variantSampleDataId", "id", "name"));
        if (CollectionUtils.isNotEmpty(formatAttrs)) {
            CollectionUtils.addAll(attrs, formatAttrs.iterator());
        }
        nodeAttributes.put(Node.Type.VARIANT_SAMPLE_DATA.toString(), attrs);

        return nodeAttributes;
    }

    private Set<String> createNoAttributes() {
        Set<String> noAttributes = new HashSet<>();
        noAttributes.add("id");
        noAttributes.add("name");
        return noAttributes;
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

//    public List<String> getSampleNames() {
//        return sampleNames;
//    }
//
//    public CsvInfo setSampleNames(List<String> sampleNames) {
//        this.sampleNames = sampleNames;
//        return this;
//    }

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

    public RocksDbManager getRocksDbManager() {
        return rocksDbManager;
    }

    public CsvInfo setRocksDbManager(RocksDbManager rocksDbManager) {
        this.rocksDbManager = rocksDbManager;
        return this;
    }

    public RocksDB getUidRocksDb() {
        return uidRocksDb;
    }

    public RocksDB getMiRnaRocksDb() {
        return miRnaRocksDb;
    }

    public GeneCache getGeneCache() {
        return geneCache;
    }

    public ProteinCache getProteinCache() {
        return proteinCache;
    }
}
