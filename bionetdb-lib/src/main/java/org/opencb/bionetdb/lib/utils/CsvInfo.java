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

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static org.opencb.bionetdb.lib.utils.Utils.PREFIX_ATTRIBUTES;

public class CsvInfo {
    public static final String SEPARATOR = "\t";
    public static final String FILENAME_SEPARATOR = "___";

    public static final String ARRAY_SEPARATOR = "|";
    public static final String MISSING_VALUE = ""; //"-";

    private long uid;
    private Path inputPath;
    private Path outputPath;

    private Map<String, BufferedWriter> csvWriters;
    private Map<String, List<String>> nodeAttributes;
    private Set<String> noAttributes;

    private RocksDbManager rocksDbManager;
    private RocksDB uidRocksDb;

    private GeneCache geneCache;
    private ProteinCache proteinCache;

    private ObjectMapper mapper;
    private ObjectReader geneReader;
    private ObjectReader proteinReader;
    private ObjectWriter objWriter;

    protected static Logger logger;

    public enum RelationFilename {

//        XREF___RNA___XREF("XREF___RNA___XREF"),
//

        HAS___GENE___TRANSCRIPT("HAS___GENE___TRANSCRIPT"),
        HAS___TRANSCRIPT___EXON("HAS___TRANSCRIPT___EXON"),
        HAS___VARIANT___STRUCTURAL_VARIANT("HAS___VARIANT___STRUCTURAL_VARIANT"),
        HAS___FEATURE_ONTOLOGY_TERM_ANNOTATION___TRANSCRIPT_ANNOTATION_EVIDENCE(
                "HAS___FEATURE_ONTOLOGY_TERM_ANNOTATION___TRANSCRIPT_ANNOTATION_EVIDENCE"),
        HAS___VARIANT_FILE___SAMPLE("HAS___VARIANT_FILE___SAMPLE"),
        HAS___FAMILY___INDIVIDUAL("HAS___FAMILY___INDIVIDUAL"),
        HAS___INDIVIDUAL___SAMPLE("HAS___INDIVIDUAL___SAMPLE"),
        HAS___DISEASE_PANEL___PANEL_GENE("HAS___DISEASE_PANEL___PANEL_GENE"),
        HAS___STRUCTURAL_VARIATION___BREAKEND("HAS___STRUCTURAL_VARIATION___BREAKEND"),
        HAS___CLINICAL_EVIDENCE___EVIDENCE_SUBMISSION("HAS___CLINICAL_EVIDENCE___EVIDENCE_SUBMISSION"),
        HAS___CLINICAL_EVIDENCE___HERITABLE_TRAIT("HAS___CLINICAL_EVIDENCE___HERITABLE_TRAIT"),
        HAS___CLINICAL_EVIDENCE___GENOMIC_FEATURE("HAS___CLINICAL_EVIDENCE___GENOMIC_FEATURE"),
        HAS___CLINICAL_EVIDENCE___VARIANT_CLASSIFICATION("HAS___CLINICAL_EVIDENCE___VARIANT_CLASSIFICATION"),
        HAS___CLINICAL_EVIDENCE___PROPERTY("HAS___CLINICAL_EVIDENCE___PROPERTY"),

        MATE___BREAKEND___BREAKEND_MATE("MATE___BREAKEND___BREAKEND_MATE"),

        MOTHER_OF___INDIVIDUAL___INDIVIDUAL("MOTHER_OF___INDIVIDUAL___INDIVIDUAL"),
        FATHER_OF___INDIVIDUAL___INDIVIDUAL("FATHER_OF___INDIVIDUAL___INDIVIDUAL"),

        TARGET___GENE___MIRNA_MATURE("TARGET___GENE___MIRNA_MATURE"),

        IS___GENE___MIRNA("IS___GENE___MIRNA"),
        IS___RNA___MIRNA("IS___RNA___MIRNA"),
        IS___DNA___GENE("IS___DNA___GENE"),
        IS___RNA___TRANSCRIPT("IS___RNA___TRANSCRIPT"),
        IS___TRANSCRIPT___PROTEIN("IS___TRANSCRIPT___PROTEIN"),

        ANNOTATION___DRUG___GENE_DRUG_INTERACTION("ANNOTATION___DRUG___GENE_DRUG_INTERACTION"),
        ANNOTATION___GENE___GENE_DRUG_INTERACTION("ANNOTATION___GENE___GENE_DRUG_INTERACTION"),
        ANNOTATION___GENE___GENE_TRAIT_ASSOCIATION("ANNOTATION___GENE___GENE_TRAIT_ASSOCIATION"),
        ANNOTATION___GENE___GENE_EXPRESSION("ANNOTATION___GENE___GENE_EXPRESSION"),
        ANNOTATION___GENE___XREF("ANNOTATION___GENE___XREF"),
        ANNOTATION___GENE___PANEL_GENE("ANNOTATION___GENE___PANEL_GENE"),
        ANNOTATION___PROTEIN___PROTEIN_KEYWORD("ANNOTATION___PROTEIN___PROTEIN_KEYWORD"),
        ANNOTATION___PROTEIN___PROTEIN_FEATURE("ANNOTATION___PROTEIN___PROTEIN_FEATURE"),
        ANNOTATION___PROTEIN___XREF("ANNOTATION___PROTEIN___XREF"),
        ANNOTATION___TRANSCRIPT___XREF("ANNOTATION___TRANSCRIPT___XREF"),
        ANNOTATION___TRANSCRIPT___TFBS("ANNOTATION___TRANSCRIPT___TFBS"),
        ANNOTATION___TRANSCRIPT___TRANSCRIPT_CONSTRAINT_SCORE("ANNOTATION___TRANSCRIPT___TRANSCRIPT_CONSTRAINT_SCORE"),
        ANNOTATION___TRANSCRIPT___FEATURE_ONTOLOGY_TERM_ANNOTATION("ANNOTATION___TRANSCRIPT___FEATURE_ONTOLOGY_TERM_ANNOTATION"),
        ANNOTATION___VARIANT___VARIANT_CONSEQUENCE_TYPE("ANNOTATION___VARIANT___VARIANT_CONSEQUENCE_TYPE"),
        ANNOTATION___VARIANT_CONSEQUENCE_TYPE___GENE("ANNOTATION___VARIANT_CONSEQUENCE_TYPE___GENE"),
        ANNOTATION___VARIANT_CONSEQUENCE_TYPE___TRANSCRIPT("ANNOTATION___VARIANT_CONSEQUENCE_TYPE___TRANSCRIPT"),
        ANNOTATION___VARIANT_CONSEQUENCE_TYPE___SO_TERM("ANNOTATION___VARIANT_CONSEQUENCE_TYPE___SO_TERM"),
        ANNOTATION___VARIANT_CONSEQUENCE_TYPE___PROTEIN_VARIANT_ANNOTATION(
                "ANNOTATION___VARIANT_CONSEQUENCE_TYPE___PROTEIN_VARIANT_ANNOTATION"),
        ANNOTATION___PROTEIN_VARIANT_ANNOTATION___PROTEIN("ANNOTATION___PROTEIN_VARIANT_ANNOTATION___PROTEIN"),
        ANNOTATION___PROTEIN_VARIANT_ANNOTATION___PROTEIN_SUBSTITUTION_SCORE(
                "ANNOTATION___PROTEIN_VARIANT_ANNOTATION___PROTEIN_SUBSTITUTION_SCORE"),
        ANNOTATION___VARIANT___HGV("ANNOTATION___VARIANT___HGV"),
        ANNOTATION___VARIANT___VARIANT_POPULATION_FREQUENCY("ANNOTATION___VARIANT___VARIANT_POPULATION_FREQUENCY"),
        ANNOTATION___VARIANT___VARIANT_CONSERVATION_SCORE("ANNOTATION___VARIANT___VARIANT_CONSERVATION_SCORE"),
        ANNOTATION___VARIANT___CLINICAL_EVIDENCE("ANNOTATION___VARIANT___CLINICAL_EVIDENCE"),
        ANNOTATION___VARIANT___VARIANT_FUNCTIONAL_SCORE("ANNOTATION___VARIANT___VARIANT_FUNCTIONAL_SCORE"),
        ANNOTATION___VARIANT___REPEAT("ANNOTATION___VARIANT___REPEAT"),
        ANNOTATION___VARIANT___CYTOBAND("ANNOTATION___VARIANT___CYTOBAND"),
        ANNOTATION___VARIANT___VARIANT_DRUG_INTERACTION("ANNOTATION___VARIANT___VARIANT_DRUG_INTERACTION"),
        ANNOTATION___VARIANT___TRANSCRIPT_CONSTRAINT_SCORE("ANNOTATION___VARIANT___TRANSCRIPT_CONSTRAINT_SCORE"),
        ANNOTATION___GENE___MIRNA_TARGET("ANNOTATION___GENE___MIRNA_TARGET"),
        ANNOTATION___MIRNA_MATURE___MIRNA_TARGET("ANNOTATION___MIRNA_MATURE___MIRNA_TARGET"),

        DATA___VARIANT___VARIANT_FILE_DATA("DATA___VARIANT___VARIANT_FILE_DATA"),
        DATA___VARIANT___VARIANT_SAMPLE_DATA("DATA___VARIANT___VARIANT_SAMPLE_DATA"),
        DATA___VARIANT_FILE___VARIANT_FILE_DATA("DATA___VARIANT_FILE___VARIANT_FILE_DATA"),
        DATA___SAMPLE___VARIANT_SAMPLE_DATA("DATA___SAMPLE___VARIANT_SAMPLE_DATA"),

        MATURE___MIRNA___MIRNA_MATURE("MATURE___MIRNA___MIRNA_MATURE"),

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
        PATHWAY_NEXT_STEP___PATHWAY___REGULATION("PATHWAY_NEXT_STEP___PATHWAY___REGULATION");

        private final String relation;

        RelationFilename(String relation) {
            this.relation = relation;
        }

        public List<RelationFilename> getAll() {
            List<RelationFilename> list = new ArrayList<>();
            return list;
        }
    }

    public CsvInfo(Path inputPath, Path outputPath) {
        uid = 1;

        this.inputPath = inputPath;
        this.outputPath = outputPath;

        csvWriters = new HashMap<>();

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
        BufferedWriter bw;
        String filename;

        noAttributes = createNoAttributes();
        nodeAttributes = createNodeAttributes(variantFiles);

        // CSV files for nodes
        for (Node.Label label : Node.Label.values()) {
            filename = label.toString() + ".csv.gz";

            bw = FileUtils.newBufferedWriter(outputPath.resolve(filename));
            csvWriters.put(label.toString(), bw);

            if (CollectionUtils.isNotEmpty(nodeAttributes.get(label.toString()))) {
                bw.write(getNodeHeaderLine(nodeAttributes.get(label.toString())));
                bw.newLine();
            }
        }

        // CSV files for relationships
        for (RelationFilename name : RelationFilename.values()) {
            filename = name.name() + ".csv.gz";
            bw = FileUtils.newBufferedWriter(outputPath.resolve(filename));

            // Write header
            bw.write(getRelationHeaderLine(name.name()));
            bw.newLine();

            // Add writer to the map
            csvWriters.put(name.name(), bw);
        }

        for (Relation.Label label : Relation.Label.values()) {
            boolean found = true;
            for (RelationFilename name : RelationFilename.values()) {
                if (name.name().startsWith(label.name())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                filename = label.name() + ".csv";
                bw = FileUtils.newBufferedWriter(outputPath.resolve(filename));

                // Write header
                bw.write(getRelationHeaderLine(label.name()));
                bw.newLine();

                // Add writer to the map
                csvWriters.put(label.name(), bw);
            }

        }

    }

    public void close() throws IOException {
        List<Map<String, BufferedWriter>> writerMaps = new ArrayList<>();
        writerMaps.add(csvWriters);

        for (Map<String, BufferedWriter> writerMap : writerMaps) {
            if (MapUtils.isNotEmpty(writerMap)) {
                Iterator<BufferedWriter> iterator = writerMap.values().iterator();
                while (iterator.hasNext()) {
                    iterator.next().close();
                }
            }
        }
    }

    public BufferedWriter getWriter(String filename) {
        return csvWriters.get(filename);
    }

    public Long getGeneUid(String xrefId) {
        Long geneUid = null;
        String geneId = geneCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(geneId)) {
            geneUid = getLong(geneId, Node.Label.GENE.name());
        } else {
            logger.info("Getting gene UID: Xref not found for gene {}", xrefId);
        }
        return geneUid;
    }

    public void saveGeneUid(String xrefId, Long geneUid) {
        String geneId = geneCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(geneId)) {
            putLong(geneId, Node.Label.GENE.name(), geneUid);
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

        putLong(geneId, Node.Label.GENE.name(), geneUid);
    }

    public Gene getGene(String xrefId) {
        return geneCache.get(xrefId);
    }

    public Long getProteinUid(String xrefId) {
        Long proteinUid = null;
        String proteinId = proteinCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(proteinId)) {
            proteinUid = getLong(proteinId, Node.Label.PROTEIN.name());
        } else {
            logger.info("Getting protein UID: Xref not found for protein {}", xrefId);
        }
        return proteinUid;
    }

    public void saveProteinUid(String xrefId, Long proteinUid) {
        String proteinId = proteinCache.getPrimaryId(xrefId);
        if (StringUtils.isNotEmpty(proteinId)) {
            putLong(proteinId, Node.Label.PROTEIN.name(), proteinUid);
        } else {
            logger.info("Setting protein UID {}: Xref not found for protein {}", proteinUid, xrefId);
        }
    }

    public void saveUnknownProteinUid(String proteinId, String proteinName, Long proteinUid) {
        proteinCache.addXrefId(proteinId, proteinId);
        if (StringUtils.isNotEmpty(proteinName)) {
            proteinCache.addXrefId(proteinName, proteinId);
        }

        putLong(proteinId, Node.Label.PROTEIN.name(), proteinUid);
    }

    public Entry getProtein(String xrefId) {
        return proteinCache.get(xrefId);
    }

    public Long getLong(String id, String type) {
        return rocksDbManager.getLong(id + "." + type, uidRocksDb);
    }

    public void putLong(String id, String type, long value) {
        rocksDbManager.putLong(id + "." + type, value, uidRocksDb);
    }


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

    // Debug purposes
    private Set<String> notdefined = new HashSet<>();

    public String nodeLine(Node node) {
        List<String> attrs = nodeAttributes.get(node.getLabels().get(0).name());
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isEmpty(attrs)) {
            if (!notdefined.contains(node.getLabels().get(0).name())) {
                System.out.println("Attributes not defined for " + node.getLabels().get(0));
                notdefined.add(node.getLabels().toString());
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
        sb.append(SEPARATOR).append(node.getLabels().get(0));
        for (int i = 1; i < node.getLabels().size(); i++) {
            sb.append(ARRAY_SEPARATOR).append(node.getLabels().get(i));
        }


//        switch (node.getLabels().get(0)) {
//            case GENE: {
//                if (node.getAttributes().containsKey("source")) {
//                    if (node.getAttributes().getString("source").equals("ensembl")) {
//                        sb.append(labelsLine(ENSEMBL_GENE));
//                    } else if (node.getAttributes().getString("source").equals("refseq")) {
//                        sb.append(labelsLine(REFSEQ_GENE));
//                    } else {
//                        sb.append(labelsLine(GENE));
//                    }
//                } else if (node.getId().startsWith("ENSG")) {
//                    sb.append(labelsLine(ENSEMBL_GENE));
//                } else {
//                    sb.append(labelsLine(GENE));
//                }
//                break;
//            }
//            case TRANSCRIPT: {
//                if (node.getAttributes().containsKey("source")) {
//                    if (node.getAttributes().getString("source").equals("ensembl")) {
//                        sb.append(labelsLine(ENSEMBL_TRANSCRIPT));
//                    } else if (node.getAttributes().getString("source").equals("refseq")) {
//                        sb.append(labelsLine(REFSEQ_TRANSCRIPT));
//                    } else {
//                        sb.append(labelsLine(TRANSCRIPT));
//                    }
//                } else if (node.getId().startsWith("ENST")) {
//                    sb.append(labelsLine(ENSEMBL_TRANSCRIPT));
//                } else {
//                    sb.append(labelsLine(TRANSCRIPT));
//                }
//                break;
//            }
//            case EXON: {
//                if (node.getAttributes().containsKey("source")) {
//                    if (node.getAttributes().getString("source").equals("ensembl")) {
//                        sb.append(labelsLine(ENSEMBL_EXON));
//                    } else if (node.getAttributes().getString("source").equals("refseq")) {
//                        sb.append(labelsLine(REFSEQ_EXON));
//                    } else {
//                        sb.append(labelsLine(EXON));
//                    }
//                } else if (node.getId().startsWith("ENSE")) {
//                    sb.append(labelsLine(ENSEMBL_EXON));
//                } else {
//                    sb.append(labelsLine(EXON));
//                }
//                break;
//            }
//            default:
//                sb.append(labelsLine(node.getLabels()));
//                break;
//        }
        return sb.toString();
    }

//    private String labelsLine(Node.Label label) {
//        StringBuilder sb = new StringBuilder();
////        sb.append(SEPARATOR).append(label.getLabels().get(0));
////        for (int i = 1; i < label.getLabels().size(); i++) {
////            sb.append(ARRAY_SEPARATOR).append(label.getLabels().get(i));
////        }
//        return sb.toString();
//    }

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

    private Map<String, List<String>> createNodeAttributes(List<File> variantFiles) {
        List<String> attrs;
        Map<String, List<String>> nodeAttributes = new HashMap<>();

        //
        // Variant related-nodes
        //

        // Variant
        attrs = Arrays.asList("variantId", "id", "name", "alternativeNames", "chromosome", "start", "end", "strand",
                "reference", "alternate", "type");
        nodeAttributes.put(Node.Label.VARIANT.toString(), new ArrayList<>(attrs));

        // Population frequency
        attrs = Arrays.asList("variantPopulationFrequencyId", "id", "name", "study", "population", "refAlleleFreq", "altAlleleFreq");
        nodeAttributes.put(Node.Label.VARIANT_POPULATION_FREQUENCY.toString(), new ArrayList<>(attrs));

        // Conservation
        attrs = Arrays.asList("variantConservationScoreId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Label.VARIANT_CONSERVATION_SCORE.toString(), new ArrayList<>(attrs));

        // HGV
        attrs = Arrays.asList("hgvId", "id", "name");
        nodeAttributes.put(Node.Label.HGV.toString(), new ArrayList<>(attrs));

        // Genomic feature
        attrs = Arrays.asList("genomicFeatureId", "id", "name", "type", "geneName", "transcriptId");
        nodeAttributes.put(Node.Label.GENOMIC_FEATURE.toString(), new ArrayList<>(attrs));

        // Functional score
        attrs = Arrays.asList("variantFunctionalScoreId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Label.VARIANT_FUNCTIONAL_SCORE.toString(), new ArrayList<>(attrs));

        // Variant drug interaction
        nodeAttributes.put(Node.Label.VARIANT_DRUG_INTERACTION.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("variantDrugInteractionId", "id", "name", "therapeuticContext", "pathway", "effect", "association", "status",
                "evidence", "bibliography");

        // Cytoband
        nodeAttributes.put(Node.Label.CYTOBAND.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("cytobandId", "id", "name", "chromosome", "start", "end", "stain");

        // Repeat
        nodeAttributes.put(Node.Label.REPEAT.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("repeatId", "id", "name", "chromosome", "start", "end", "period", "consensusSize", "copyNumber",
                "percentageMatch", "score", "source");

        // Structural variation
        nodeAttributes.put(Node.Label.STRUCTURAL_VARIATION.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("structuralVariationId", "id", "name", "ciStartLeft", "ciStartRight", "ciEndLeft", "ciEndRight", "copyNumber",
                "leftSvInSeq", "rightSvInSeq",
                "type");

        // Breakend
        nodeAttributes.put(Node.Label.BREAKEND.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("breakendId", "id", "name", "orientation");

        // Breakend mate
        nodeAttributes.put(Node.Label.BREAKEND_MATE.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("breakendMateId", "id", "name", "chromosome", "position", "ciPositionLeft", "ciPositionRight");

        // Clinical evidence
        nodeAttributes.put(Node.Label.CLINICAL_EVIDENCE.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("clinicalEvidenceId", "id", "name", "url", "sourceName", "sourceVersion", "sourceDate", "alleleOrigin",
                "primarySite", "siteSubtype", "primaryHistology", "histologySubtype", "tumorOrigin", "sampleSource", "impact", "confidence",
                "consistencyStatus", "ethnicity", "penetrance", "variableExpressivity", "description", "bibliography");

        // Evidence submission
        nodeAttributes.put(Node.Label.EVIDENCE_SUBMISSION.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("evidenceSubmissionId", "id", "name", "submitter", "date");

        // Heritable trait
        nodeAttributes.put(Node.Label.HERITABLE_TRAIT.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("heritableTraitId", "id", "name", "trait", "inheritanceMode");

        // Property
        nodeAttributes.put(Node.Label.PROPERTY.toString(), new ArrayList<>(attrs));
        attrs = Arrays.asList("propertyId", "id", "name", "value");

        // Variant classification
        attrs = Arrays.asList("variantClassificationId", "id", "name", "acmg", "clinicalSignificance", "drugResponse", "traitAssociation",
                "functionalEffect", "tumorigenesis");
        nodeAttributes.put(Node.Label.VARIANT_CLASSIFICATION.toString(), new ArrayList<>(attrs));

        // Consequence type
        attrs = Arrays.asList("variantConsequenceTypeId", "id", "name", "study", "biotype", "cdnaPosition", "cdsPosition", "codon",
                "strand", "gene", "transcript", "transcriptAnnotationFlags", "exonOverlap");
        nodeAttributes.put(Node.Label.VARIANT_CONSEQUENCE_TYPE.toString(), new ArrayList<>(attrs));

        // Protein variant annotation
        attrs = Arrays.asList("proteinVariantAnnotationId", "id", "name", "position", "reference", "alternate",
                "functionalDescription");
        nodeAttributes.put(Node.Label.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        // File
        attrs = Arrays.asList("fileId", "id", "name");
        nodeAttributes.put(Node.Label.VARIANT_FILE.toString(), new ArrayList<>(attrs));

        // Family
        attrs = Arrays.asList("familyId", "id", "name");
        nodeAttributes.put(Node.Label.FAMILY.toString(), new ArrayList<>(attrs));

        // Individual
        attrs = Arrays.asList("individualId", "id", "name", "sex", "phenotype");
        nodeAttributes.put(Node.Label.INDIVIDUAL.toString(), new ArrayList<>(attrs));

        // Sample, variant file info and variant sample format
        nodeAttributes.putAll(createSampleRelatedAttrs(variantFiles));

        //
        // Gene related-nodes
        //

        // Gene
        attrs = Arrays.asList("geneId", "id", "name", "biotype", "chromosome", "start", "end", "strand", "description",
                "version", "source", "status");
        nodeAttributes.put(Node.Label.GENE.toString(), new ArrayList<>(attrs));

        // Disease panel
        attrs = Arrays.asList("diseasePanelId", "id", "name", "description", "phenotypeNames", "sourceId", "sourceName",
                "sourceAuthor", "sourceProject", "sourceVersion", "creationDate", "modificationDate");
        nodeAttributes.put(Node.Label.DISEASE_PANEL.toString(), new ArrayList<>(attrs));

        // Panel gene
        attrs = Arrays.asList("panelGeneId", "id", "name", "modeOfInheritance", "penetrance", "confidence", "evidences", "publications",
                "coordinates");
        nodeAttributes.put(Node.Label.PANEL_GENE.toString(), new ArrayList<>(attrs));

        // Gene drug interaction
        attrs = Arrays.asList("geneDrugInteractionId", "id", "name", "source", "type", "studyType", "chemblId");
        nodeAttributes.put(Node.Label.GENE_DRUG_INTERACTION.toString(), new ArrayList<>(attrs));

        // Drug
        attrs = Arrays.asList("drugId", "id", "name");
        nodeAttributes.put(Node.Label.DRUG.toString(), new ArrayList<>(attrs));

        // Gene trait association
        attrs = Arrays.asList("geneTraitAssociationId", "id", "name", "hpo", "numberOfPubmeds", "score", "source", "sources",
                "associationType");
        nodeAttributes.put(Node.Label.GENE_TRAIT_ASSOCIATION.toString(), new ArrayList<>(attrs));

        // Gene expression
        attrs = Arrays.asList("geneExpressionId", "id", "name", "transcriptId", "experimentalFactor", "factorValue", "experimentId",
                "technologyPlatform", "expressionCall", "pValue");
        nodeAttributes.put(Node.Label.GENE_EXPRESSION.toString(), new ArrayList<>(attrs));

        // Transcript
        attrs = Arrays.asList("transcriptId", "id", "name", "chromosome", "start", "end", "strand", "biotype", "status",
                "genomicCodingStart", "genomicCodingEnd", "cdnaCodingStart", "cdnaCodingEnd", "cdsLength", "description", "version",
                "source", "annotationFlags");
        nodeAttributes.put(Node.Label.TRANSCRIPT.toString(), new ArrayList<>(attrs));

        // Exon
        attrs = Arrays.asList("exonId", "id", "name", "chromosome", "start", "end", "strand", "genomicCodingStart",
                "genomicCodingEnd", "cdnaCodingStart", "cdnaCodingEnd", "cdsStart", "cdsEnd", "phase", "exonNumber", "source");
        nodeAttributes.put(Node.Label.EXON.toString(), new ArrayList<>(attrs));

        // Constraint
        attrs = Arrays.asList("transcriptConstraintScoreId", "id", "name", "source", "method", "value");
        nodeAttributes.put(Node.Label.TRANSCRIPT_CONSTRAINT_SCORE.toString(), new ArrayList<>(attrs));

        // Feature ontology term annotation
        attrs = Arrays.asList("featureOntologyTermAnnotationId", "id", "name", "source", "attributes");
        nodeAttributes.put(Node.Label.FEATURE_ONTOLOGY_TERM_ANNOTATION.toString(), new ArrayList<>(attrs));

        // Annotation evidence
        attrs = Arrays.asList("transcriptAnnotationEvidenceId", "id", "name", "references", "qualifier");
        nodeAttributes.put(Node.Label.TRANSCRIPT_ANNOTATION_EVIDENCE.toString(), new ArrayList<>(attrs));

        // miRNA
        attrs = Arrays.asList("miRnaId", "id", "name", "accession", "status", "sequence");
        nodeAttributes.put(Node.Label.MIRNA.toString(), new ArrayList<>(attrs));

        // miRNA mature
        attrs = Arrays.asList("miRnaMatureId", "id", "name", "accession", "sequence", "start", "end");
        nodeAttributes.put(Node.Label.MIRNA_MATURE.toString(), new ArrayList<>(attrs));

        // miRNA target
        attrs = Arrays.asList("miRnaTargetId", "id", "name", "experiment", "evidence", "pubmed");
        nodeAttributes.put(Node.Label.MIRNA_TARGET.toString(), new ArrayList<>(attrs));

        // TFBS
        attrs = Arrays.asList("tfbsId", "id", "name", "chromosome", "start", "end", "strand", "relativeStart",
                "relativeEnd", "score", "pwm");
        nodeAttributes.put(Node.Label.TFBS.toString(), new ArrayList<>(attrs));

        // Xref
        attrs = Arrays.asList("xrefId", "id", "name", "dbName", "dbDisplayName", "description");
        nodeAttributes.put(Node.Label.XREF.toString(), new ArrayList<>(attrs));

        // SO_TERM
        attrs = Arrays.asList("soTermId", "id", "name");
        nodeAttributes.put(Node.Label.SO_TERM.toString(), new ArrayList<>(attrs));

        //
        // Protein related-nodes
        //

        // Protein
        attrs = Arrays.asList("protId", "id", "name", "accession", "dataset", "proteinExistence", "evidence", "object",
                "xrefIds", "xrefDbs");
        nodeAttributes.put(Node.Label.PROTEIN.toString(), new ArrayList<>(attrs));

        // Protein keyword
        attrs = Arrays.asList("proteinKeywordId", "id", "name", "evidence");
        nodeAttributes.put(Node.Label.PROTEIN_KEYWORD.toString(), new ArrayList<>(attrs));

        // Protein feature
        attrs = Arrays.asList("proteinFeatureId", "id", "name", "type", "evidence", "locationPosition", "locationBegin",
                "locationEnd", "description");
        nodeAttributes.put(Node.Label.PROTEIN_FEATURE.toString(), new ArrayList<>(attrs));

        // Protein variant annotation
        attrs = Arrays.asList("proteinVariantAnnotationId", "id", "name");
        nodeAttributes.put(Node.Label.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        // Substitution score
        attrs = Arrays.asList("proteinSubstitutionScoreId", "id", "name", "score");
        nodeAttributes.put(Node.Label.PROTEIN_SUBSTITUTION_SCORE.toString(), new ArrayList<>(attrs));

        //
        // Pathway related-nodes
        //

        // Cellular location
        attrs = Arrays.asList("cellularLocationId", "id", "name");
        nodeAttributes.put(Node.Label.CELLULAR_LOCATION.toString(), new ArrayList<>(attrs));

        // Pathway
        attrs = Arrays.asList("pathwayId", "id", "name");
        nodeAttributes.put(Node.Label.PATHWAY.toString(), new ArrayList<>(attrs));

        // Small molecule
        attrs = Arrays.asList("smallMoleculeId", "id", "name");
        nodeAttributes.put(Node.Label.SMALL_MOLECULE.toString(), new ArrayList<>(attrs));

        // RNA
        attrs = Arrays.asList("rnaId", "id", "name", "evidence", "xrefIds", "xrefDbs");
        nodeAttributes.put(Node.Label.RNA.toString(), new ArrayList<>(attrs));

        // catalysis
        attrs = Arrays.asList("catalysisId", "id", "name");
        nodeAttributes.put(Node.Label.CATALYSIS.toString(), new ArrayList<>(attrs));

        // complex
        attrs = Arrays.asList("physicalEntityComplexId", "id", "name");
        nodeAttributes.put(Node.Label.PHYSICAL_ENTITY_COMPLEX.toString(), new ArrayList<>(attrs));

        // reaction
        attrs = Arrays.asList("reactionId", "id", "name");
        nodeAttributes.put(Node.Label.REACTION.toString(), new ArrayList<>(attrs));

        // DNA
        attrs = Arrays.asList("dnaId", "id", "name", "xrefIds", "xrefDbs");
        nodeAttributes.put(Node.Label.DNA.toString(), new ArrayList<>(attrs));

        // Undefined
        attrs = Arrays.asList("undefinedId", "id", "name");
        nodeAttributes.put(Node.Label.UNDEFINED.toString(), new ArrayList<>(attrs));

        // Regulation
        attrs = Arrays.asList("regulationId", "id", "name");
        nodeAttributes.put(Node.Label.REGULATION.toString(), new ArrayList<>(attrs));

        //
        // Internal config
        //

        attrs = Arrays.asList("internalConfigId", "id", "name", "uidCounter");
        nodeAttributes.put(Node.Label.INTERNAL_CONNFIG.toString(), new ArrayList<>(attrs));

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

                    if (variantStudyMetadata.getAggregatedHeader() != null) {
                        for (VariantFileHeaderComplexLine line : variantStudyMetadata.getAggregatedHeader().getComplexLines()) {
                            if ("INFO".equals(line.getKey())) {
                                infoAttrs.add(line.getId());
                            } else if ("FORMAT".equals(line.getKey())) {
                                formatAttrs.add(line.getId());
                            }
                        }
                    } else {
                        if (CollectionUtils.isNotEmpty(variantStudyMetadata.getFiles())) {
                            for (VariantFileMetadata variantFileMetadata : variantStudyMetadata.getFiles()) {
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
        }

        // Sample
        attrs = new ArrayList<>();
        attrs.addAll(Arrays.asList("sampleId", "id", "name"));
        if (CollectionUtils.isNotEmpty(sampleAttrs)) {
            CollectionUtils.addAll(attrs, sampleAttrs.iterator());
        }
        nodeAttributes.put(Node.Label.SAMPLE.toString(), attrs);

        // Variant file info
        attrs = new ArrayList<>();
        attrs.addAll(Arrays.asList("variantFileDataId", "id", "name"));
        if (CollectionUtils.isNotEmpty(infoAttrs)) {
            CollectionUtils.addAll(attrs, infoAttrs.iterator());
        }
        nodeAttributes.put(Node.Label.VARIANT_FILE_DATA.toString(), attrs);

        // Variant sample format
        attrs = new ArrayList<>();
        attrs.addAll(Arrays.asList("variantSampleDataId", "id", "name"));
        if (CollectionUtils.isNotEmpty(formatAttrs)) {
            CollectionUtils.addAll(attrs, formatAttrs.iterator());
        }
        nodeAttributes.put(Node.Label.VARIANT_SAMPLE_DATA.toString(), attrs);

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

    public Path getOutputPath() {
        return outputPath;
    }

    public CsvInfo setOutputPath(Path outputPath) {
        this.outputPath = outputPath;
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

    public GeneCache getGeneCache() {
        return geneCache;
    }

    public ProteinCache getProteinCache() {
        return proteinCache;
    }
}
