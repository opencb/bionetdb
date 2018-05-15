package org.opencb.bionetdb.core.utils;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.commons.utils.ListUtils;
import org.rocksdb.RocksDB;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.*;

public class CSVInfo {
    public static final String SEPARATOR = ",";
    private static final String MISSING_VALUE = "-";

    private long uid;
    private Path outputPath;

    private Map<String, PrintWriter> csvWriters;
    private Map<String, PrintWriter> csvAnnotatedWriters;
    private Map<String, List<String>> nodeAttributes;

    private Set<String> formatFields;
    private Set<String> infoFields;

    private RocksDBManager rocksDBManager;
    private RocksDB rocksDB;

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

        XREF___PROTEIN___XREF("XREF___PROTEIN___XREF");

        private final String relation;

        BioPAXRelation(String relation) {
            this.relation = relation;
        }

        public List<BioPAXRelation> getAll() {
            List<BioPAXRelation> list = new ArrayList<>();
            return list;
        }
    }

    public CSVInfo(Path outputPath) {
        this.uid = 1;
        //this.bioPAXImporter = new Neo4JBioPAXImporter()

        this.outputPath = outputPath;

        this.csvWriters = new HashMap<>();
        this.csvAnnotatedWriters = new HashMap<>();
        this.nodeAttributes = createNodeAttributes();

        this.rocksDBManager = new RocksDBManager();
        this.rocksDB = this.rocksDBManager.getDBConnection(outputPath.toString() + "/rocksdb", true);
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

//            pw = new PrintWriter(new BufferedWriter(new FileWriter(outputPath + "/" + filename, !header)));
            pw = new PrintWriter(outputPath + "/" + filename);
            csvWriters.put(type.toString(), pw);

            if (ListUtils.isNotEmpty(nodeAttributes.get(type.toString()))) {
                pw.println(getNodeHeaderLine(nodeAttributes.get(type.toString())));
            }
        }

        // For annotating purpose
        List<String> types = new ArrayList<>();
        types.add(Node.Type.GENE.toString());
        types.add(Node.Type.TRANSCRIPT.toString());
        for (String type : types) {
            filename = type + ".csv.annotated";
            pw = new PrintWriter(outputPath + "/" + filename);
            csvAnnotatedWriters.put(type, pw);
            if (ListUtils.isNotEmpty(nodeAttributes.get(type))) {
                pw.println(getNodeHeaderLine(nodeAttributes.get(type)));
            }
        }

        // CSV files for relationships
        for (Relation.Type type : Relation.Type.values()) {
            if (type.toString().contains("__")) {
                filename = type.toString() + ".csv";
//                pw = new PrintWriter(new BufferedWriter(new FileWriter(outputPath + "/" + filename, !header)));
                pw = new PrintWriter(outputPath + "/" + filename);
                csvWriters.put(type.toString(), pw);

                pw.println(getRelationHeaderLine(type.toString()));
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

    public Long getLong(String key) {
        return rocksDBManager.getLong(key, rocksDB);
    }

    public String getString(String key) {
        return rocksDBManager.getString(key, rocksDB);
    }

    public void putLong(String key, long value) {
        rocksDBManager.putLong(key, value, rocksDB);
    }

    public void putString(String key, String value) {
        rocksDBManager.putString(key, value, rocksDB);
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

    private Map<String, List<String>> createNodeAttributes() {
        List<String> attrs;
        Map<String, List<String>> nodeAttributes = new HashMap<>();

        //variant: (uid:ID(variantId),id,name,chromosome,start,end,reference,alternate,strand,type)
        attrs = Arrays.asList("variantId", "id", "name", "alternativeNames", "chromosome", "start", "end", "strand",
                "reference", "alternate", "type");
        nodeAttributes.put(Node.Type.VARIANT.toString(), new ArrayList<>(attrs));

        //population frequency: (uid:ID(popFreqId),id,name,study,population,refAlleleFreq,altAlleleFreq)
        attrs = Arrays.asList("popFreqId", "id", "name", "study", "population", "refAlleleFreq", "altAlleleFreq");
        nodeAttributes.put(Node.Type.POPULATION_FREQUENCY.toString(), new ArrayList<>(attrs));

        //conservation: (uid:ID(consId),id,name,score,source,description)
        attrs = Arrays.asList("consId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Type.CONSERVATION.toString(), new ArrayList<>(attrs));

        //functional score: (uid:ID(consId),id,name,score,source,description)
        attrs = Arrays.asList("funcScoreId", "id", "name", "score", "source", "description");
        nodeAttributes.put(Node.Type.FUNCTIONAL_SCORE.toString(), new ArrayList<>(attrs));

        //trait association: (uid:ID(traitId),name,url,heritableTraits,source,alleleOrigin)
        attrs = Arrays.asList("traitId", "id", "name", "url", "heritableTraits", "source", "alleleOrigin");
        nodeAttributes.put(Node.Type.TRAIT_ASSOCIATION.toString(), new ArrayList<>(attrs));

        //consequence type: (uid:ID(ctId),id,name,biotype,cdnaPosition,cdsPosition,codon,strand,gene,transcript,
        // transcriptAnnotationFlags,exonOverlap)
        attrs = Arrays.asList("consTypeId", "id", "name", "study", "biotype", "cdnaPosition", "cdsPosition", "codon",
                "strand", "gene", "transcript", "transcriptAnnotationFlags", "exonOverlap");
        nodeAttributes.put(Node.Type.CONSEQUENCE_TYPE.toString(), new ArrayList<>(attrs));

        //protein variant annotation: (uid:ID(protAnnId),id,name,position,reference,alternate,functionalDescription)
        attrs = Arrays.asList("protVarAnnoId", "id", "name", "position", "reference", "alternate",
                "functionalDescription");
        nodeAttributes.put(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        //gene: (uid:ID(geneId),id,name,biotype,chromosome,start,end,strand,description,source,status)
        attrs = Arrays.asList("geneId", "id", "name", "biotype", "chromosome", "start", "end", "strand", "description",
                "source", "status");
        nodeAttributes.put(Node.Type.GENE.toString(), new ArrayList<>(attrs));

        //drug: (uid:ID(drugId),id,name,source,type,studyType)
        attrs = Arrays.asList("drugId", "id", "name", "source", "type", "studyType");
        nodeAttributes.put(Node.Type.DRUG.toString(), new ArrayList<>(attrs));

        //disease: (uid:ID(diseaseId),id,name,hpo,numberOfPubmeds,score,source,sources,associationType)
        attrs = Arrays.asList("diseaseId", "id", "name", "hpo", "numberOfPubmeds", "score", "source", "sources",
                "associationType");
        nodeAttributes.put(Node.Type.DISEASE.toString(), new ArrayList<>(attrs));

        //transcript: (uid:ID(transcriptId),id,name,proteinId,biotype,chromosome,start,end,strand,status,
        // cdnaCodingStart,cdnaCodingEnd,genomicCodingStart,genomicCodingEnd,cdsLength,description,annotationFlags
        attrs = Arrays.asList("transcriptId", "id", "name", "proteinId", "biotype", "chromosome", "start", "end",
                "strand", "status", "cdnaCodingStart", "cdnaCodingEnd", "genomicCodingStart", "genomicCodingEnd",
                "cdsLength", "description", "annotationFlags");
        nodeAttributes.put(Node.Type.TRANSCRIPT.toString(), new ArrayList<>(attrs));

        //tfbs: (uid:ID(tfbsId),id,name,chromosome,start,end,strand,relativeStart,relativEnd,score,pwm)
        attrs = Arrays.asList("tfbsId", "id", "name", "chromosome", "start", "end", "strand", "relativeStart",
                "relativeEnd", "score", "pwm");
        nodeAttributes.put(Node.Type.TFBS.toString(), new ArrayList<>(attrs));

        //xref: (uid:ID(xrefId),id,name,dbName,dbDisplayName,description)
        attrs = Arrays.asList("xrefId", "id", "name", "dbName", "dbDisplayName", "description");
        nodeAttributes.put(Node.Type.XREF.toString(), new ArrayList<>(attrs));

        //protein: (uid:ID(proteinId),id,name,accession,dataset,dbReference,proteinExistence,evidence
        attrs = Arrays.asList("protId", "id", "name", "accession", "dataset", "dbReference", "proteinExistence",
                "evidence");
        nodeAttributes.put(Node.Type.PROTEIN.toString(), new ArrayList<>(attrs));

        //keyword: (uid:ID(kwId),id,name,evidence)
        attrs = Arrays.asList("kwId", "id", "name", "evidence");
        nodeAttributes.put(Node.Type.PROTEIN_KEYWORD.toString(), new ArrayList<>(attrs));

        //feature: (uid:ID(featureId),id,name,evidence,location_position,location_begin,location_end,description)
        attrs = Arrays.asList("protFeatureId", "id", "name", "evidence", "location_position", "location_begin",
                "location_end", "description");
        nodeAttributes.put(Node.Type.PROTEIN_FEATURE.toString(), new ArrayList<>(attrs));

        //sample: (uid:ID(sampleId),id,name)
        attrs = Arrays.asList("sampleId", "id", "name");
        nodeAttributes.put(Node.Type.SAMPLE.toString(), new ArrayList<>(attrs));

        //variantCall: (uid:ID(variantCallId),id,name)
        attrs = Arrays.asList("variantCallId", "id", "name");
        nodeAttributes.put(Node.Type.VARIANT_CALL.toString(), new ArrayList<>(attrs));

        //variantFileInfo: (uid:ID(variantFileInfoId),id,name)
        attrs = Arrays.asList("variantFileInfoId", "id", "name");
        nodeAttributes.put(Node.Type.VARIANT_FILE_INFO.toString(), new ArrayList<>(attrs));

        //so: (uid:ID(soId),id,name)
        attrs = Arrays.asList("soId", "id", "name");
        nodeAttributes.put(Node.Type.SO.toString(), new ArrayList<>(attrs));

        //proteinVariantAnnotation: (uid:ID(protVarAnnoId),id,name)
        attrs = Arrays.asList("protVarAnnoId", "id", "name");
        nodeAttributes.put(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString(), new ArrayList<>(attrs));

        //substitutionScore: (uid:ID(substScoreId),id,name)
        attrs = Arrays.asList("substScoreId", "id", "name", "score");
        nodeAttributes.put(Node.Type.SUBSTITUTION_SCORE.toString(), new ArrayList<>(attrs));

        //
        // BIO PAX: nodes
        //

        // cellularLocation
        attrs = Arrays.asList("cellularLocationId", "id", "name");
        nodeAttributes.put(Node.Type.CELLULAR_LOCATION.toString(), new ArrayList<>(attrs));

        // pathway
        attrs = Arrays.asList("pathwayId", "id", "name");
        nodeAttributes.put(Node.Type.PATHWAY.toString(), new ArrayList<>(attrs));

        // smallMolecule
        attrs = Arrays.asList("smallMoleculeId", "id", "name");
        nodeAttributes.put(Node.Type.SMALL_MOLECULE.toString(), new ArrayList<>(attrs));

        // rna
        attrs = Arrays.asList("rnaId", "id", "name");
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

        // dna
        attrs = Arrays.asList("dnaId", "id", "name");
        nodeAttributes.put(Node.Type.DNA.toString(), new ArrayList<>(attrs));

        // undefined
        attrs = Arrays.asList("undefinedId", "id", "name");
        nodeAttributes.put(Node.Type.UNDEFINED.toString(), new ArrayList<>(attrs));

        // regulation
        attrs = Arrays.asList("regulationId", "id", "name");
        nodeAttributes.put(Node.Type.REGULATION.toString(), new ArrayList<>(attrs));

        //
        // BIO PAX: relations
        //

        return nodeAttributes;
    }

    public long getUid() {
        return uid;
    }

    public CSVInfo setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public static String getSEPARATOR() {
        return SEPARATOR;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public CSVInfo setOutputPath(Path outputPath) {
        this.outputPath = outputPath;
        return this;
    }

    public Map<String, PrintWriter> getCsvWriters() {
        return csvWriters;
    }

    public CSVInfo setCsvWriters(Map<String, PrintWriter> csvWriters) {
        this.csvWriters = csvWriters;
        return this;
    }

    public Map<String, PrintWriter> getCsvAnnotatedWriters() {
        return csvAnnotatedWriters;
    }

    public CSVInfo setCsvAnnotatedWriters(Map<String, PrintWriter> csvAnnotatedWriters) {
        this.csvAnnotatedWriters = csvAnnotatedWriters;
        return this;
    }

    public Map<String, List<String>> getNodeAttributes() {
        return nodeAttributes;
    }

    public CSVInfo setNodeAttributes(Map<String, List<String>> nodeAttributes) {
        this.nodeAttributes = nodeAttributes;
        return this;
    }

    public Set<String> getFormatFields() {
        return formatFields;
    }

    public CSVInfo setFormatFields(Set<String> formatFields) {
        this.formatFields = formatFields;
        return this;
    }

    public Set<String> getInfoFields() {
        return infoFields;
    }

    public CSVInfo setInfoFields(Set<String> infoFields) {
        this.infoFields = infoFields;
        return this;
    }

    public RocksDBManager getRocksDBManager() {
        return rocksDBManager;
    }

    public CSVInfo setRocksDBManager(RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
        return this;
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    public CSVInfo setRocksDB(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        return this;
    }
}
