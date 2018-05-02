package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class Neo4JImporter {

    private final int VARIANT_BATCH_SIZE = 200;

    private final String SEPARATOR = ",";
//    private final String NODE_PREFIX = "n.";
//    private final String RELATION_PREFIX = "r.";

    private long uid = 0;
    private CSVforVariant csvforVariant;
    private Map<String, List<String>> nodeAttributes;

    private Set<String> geneSet;
    private Set<String> transcriptSet;
    private Set<String> soSet;
    private Set<String> kwSet;
    private Set<String> featureSet;
    private Set<String> proteinSet;
    private Set<String> drugSet;

    private class CSVforVariant {
        private Map<String, String> csvFilenameMap;
        private Map<String, PrintWriter> csvWriterMap;

        private Set<String> formatSet;
        private Set<String> infoSet;

        private Path vcfPath;
        private Path outPath;
        private VCFHeader vcfHeader;

        CSVforVariant(Path vcfPath) throws FileNotFoundException {
            this.vcfPath = vcfPath;

            String name = vcfPath.toFile().getName();

            csvFilenameMap = new HashMap<>();
            csvWriterMap = new HashMap<>();

            // Insert nodes into the filename map
            for (Node.Type type: Node.Type.values()) {
                //csvFilenameMap.put(NODE_PREFIX + type, name + ".n." + type + ".csv");
                //csvFilenameMap.put(type.toString(), name + "." + type + ".csv");
                csvFilenameMap.put(type.toString(), type + ".csv");
            }

            // Insert relationships into the filename map
            for (Relation.Type type: Relation.Type.values()) {
                if (type.toString().contains("__")) {
                    csvFilenameMap.put(type.toString(), type + ".csv");
                }
            }

            formatSet = new LinkedHashSet<>();
            infoSet = new LinkedHashSet<>();
        }

        public void open(Path outPath) throws FileNotFoundException {
            this.outPath = outPath;

            for (String key: csvFilenameMap.keySet()) {
                csvWriterMap.put(key, new PrintWriter(outPath + "/" + csvFilenameMap.get(key)));
            }
        }

        public void writeHeaders(VCFHeader vcfHeader) {
            this.vcfHeader = vcfHeader;

            PrintWriter pw;

            // Node headers
            for (Node.Type type: Node.Type.values()) {
                if (nodeAttributes.containsKey(type)) {
                    if (type != Node.Type.VARIANT_CALL && type != Node.Type.VARIANT_FILE_INFO) {
                        pw = csvWriterMap.get(type.toString());
                        pw.println(getNodeHeaderLine(nodeAttributes.get(type.toString())));
                    }
                }
            }

            // VARIANT_CALL nodes
            StringBuilder sb = new StringBuilder();
            Collection<VCFFormatHeaderLine> formatHeaderLines = vcfHeader.getFormatHeaderLines();
            sb.setLength(0);
            for (VCFFormatHeaderLine formatHeaderLine: vcfHeader.getFormatHeaderLines()) {
                formatSet.add(formatHeaderLine.getID());
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append("attr_").append(formatHeaderLine.getID());
            }
            pw = csvWriterMap.get(Node.Type.VARIANT_CALL.toString());
            pw.print("id:ID(variantCallId),name");
            if (sb.length() > 0) {
                pw.print(",");
                pw.println(sb.toString());
            } else {
                pw.println("");
            }

            // VARIANT_FILE_INFO nodes
            pw = csvWriterMap.get(Node.Type.VARIANT_FILE_INFO.toString());
            Collection<VCFInfoHeaderLine> infoHeaderLines = vcfHeader.getInfoHeaderLines();
            sb.setLength(0);
            for (VCFInfoHeaderLine infoHeaderLine: infoHeaderLines) {
                infoSet.add(infoHeaderLine.getID());
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append("attr_").append(infoHeaderLine.getID());
            }
            pw.print("id:ID(variantFileInfoId),name,attr_filename");
            if (sb.length() > 0) {
                pw.print(",");
                pw.println(sb.toString());
            } else {
                pw.println("");
            }

            // Relation headers
            for (Relation.Type type: Relation.Type.values()) {
                if (type.toString().contains("__")) {
                    pw = csvWriterMap.get(type.toString());
                    pw.println(getRelationHeaderLine(type.toString()));
                }
            }
        }

        public void writeHeaders() {
            this.vcfHeader = vcfHeader;

            PrintWriter pw;

            // Nodes
            for (Node.Type type: Node.Type.values()) {
                if (nodeAttributes.containsKey(type.toString())) {
                    pw = csvWriterMap.get(type.toString());
                    pw.println(getNodeHeaderLine(nodeAttributes.get(type.toString())));
                }
            }

            // Relation headers
            for (Relation.Type type: Relation.Type.values()) {
                if (type.toString().contains("__")) {
                    pw = csvWriterMap.get(type.toString());
                    pw.println(getRelationHeaderLine(type.toString()));
                }
            }
        }

        public void close() {
            for (String key: csvFilenameMap.keySet()) {
                csvWriterMap.get(key).close();
            }
        }
    }

    public Neo4JImporter() {
        List<String> attrs;

        nodeAttributes = new HashMap<>();

        geneSet = new HashSet<>();
        transcriptSet = new HashSet<>();
        soSet = new HashSet<>();
        kwSet = new HashSet<>();
        featureSet = new HashSet<>();
        proteinSet = new HashSet();
        drugSet = new HashSet<>();

        //variant: (uid:ID(variantId),id,name,chromosome,start,end,reference,alternate,strand,type)
        attrs = Arrays.asList("variantId", "id", "name", "chromosome", "start", "end", "strand", "reference",
                "alternate", "type");
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
    }

    public void generateCSVFromVCF(Path vcfPath, Path outDir) throws FileNotFoundException {
        this.csvforVariant = new CSVforVariant(vcfPath);

        // VCF File reader management
        VcfFileReader vcfFileReader = new VcfFileReader(vcfPath.toString(), false);
        vcfFileReader.open();
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        csvforVariant.open(outDir);
        csvforVariant.writeHeaders(vcfHeader);

        // sample.cvs for SAMPLE nodes
        List<String> sampleNames = vcfHeader.getSampleNamesInOrder();
        PrintWriter pw = csvforVariant.csvWriterMap.get(Node.Type.SAMPLE.toString());
        for (String sampleName: sampleNames) {
            pw.println(sampleName + "," + sampleName);
        }
        pw.close();

        // VariantContext-to-Variant converter
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter("dataset",
                vcfPath.toFile().getName(), vcfFileReader.getVcfHeader().getSampleNamesInOrder());

        List<VariantContext> variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        while (variantContexts.size() == VARIANT_BATCH_SIZE) {
            updateCSVVariantFiles(Neo4JConverter.convert(variantContexts, converter), csvforVariant);

            // Read next batch
            variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        }
        if (variantContexts.size() > 0) {
            updateCSVVariantFiles(Neo4JConverter.convert(variantContexts, converter), csvforVariant);
        }

        // Close CSV files
        csvforVariant.close();

        // close VCF file reader
        vcfFileReader.close();
    }

    public void generateCSVFromJSON(Path inputPath, Path outDir) throws IOException {
        this.csvforVariant = new CSVforVariant(inputPath);

        // Create and open CSV files
        csvforVariant.open(outDir);
        csvforVariant.writeHeaders();

        // Reading file line by line, each line a JSON object
        BufferedReader reader;
        ObjectMapper mapper = new ObjectMapper();

        reader = FileUtils.newBufferedReader(inputPath); //new BufferedReader(new FileReader(inputPath.toString()));
        String line = reader.readLine();
        List<Variant> variants = new ArrayList<>();
        while (line != null) {
            Variant variant = mapper.readValue(line, Variant.class);
            variants.add(variant);
            if (variants.size() > 100) {
                updateCSVVariantFiles(variants, csvforVariant);
                variants.clear();
            }
            // read next line
            line = reader.readLine();
        }
        reader.close();
        if (variants.size() > 0) {
            updateCSVVariantFiles(variants, csvforVariant);
            variants.clear();
        }

        // Close CSV files
        csvforVariant.close();
    }

    public void importCSV(Path neo4jHome) throws IOException, InterruptedException {
        String home = neo4jHome == null ? System.getenv("NEO4J_HOME") : neo4jHome.toString();
        if (home == null) {
            throw new InterruptedException("Neo4j's home directory is missing. To fix it, please, set the environment "
                    + "variable NEO4J_HOME.");
        }

        String neo4j = home + "/bin/neo4j";
        String neo4jAdmin = home + "/bin/neo4j-admin";

        Runtime rt = Runtime.getRuntime();
        System.out.println("Stopping Neo4j...");
        Process pr = rt.exec(neo4j + " stop; rm -rf " + home + "/data/databases/graph.db");
        pr.waitFor();
        System.out.println("\t...done!");

        StringBuilder sb = new StringBuilder();
        sb.append(neo4jAdmin).append(" import");
        String outDir = csvforVariant.outPath.toString();
        for (String key: csvforVariant.csvFilenameMap.keySet()) {
            if (key.contains("__")) {
                sb.append(" --relationships:").append(key).append(" ").append(outDir).append("/")
                        .append(csvforVariant.csvFilenameMap.get(key));
            } else {
                sb.append(" --nodes:").append(key).append(" ").append(outDir).append("/")
                        .append(csvforVariant.csvFilenameMap.get(key));
            }
        }
//                .append(" --nodes:SAMPLE ").append(csvforVariant.outPath).append("/")
// .append(csvforVariant.sampleCSV)
//                .append(" --nodes:VARIANT ").append(csvforVariant.outPath).append("/")
// .append(csvforVariant.variantCSV)
//                .append(" --nodes:VARIANT_CALL ").append(csvforVariant.outPath).append("/")
// .append(csvforVariant.variantcallCSV)
//                .append(" --nodes:VARIANT_FILE_INFO ").append(csvforVariant.outPath).append("/")
// .append(csvforVariant.variantfileinfoCSV)
//                .append(" --relationships:VARIANT_CALL ").append(csvforVariant.outPath).append("/")
// .append(csvforVariant.variant_variantcallCSV)
//                .append(" --relationships:VARIANT_CALL ").append(csvforVariant.outPath).append("/")
// .append(csvforVariant.sample_variantcallCSV)
//                .append(" --relationships:VARIANT_FILE_INFO ").append(csvforVariant.outPath).append("/")
//                .append(csvforVariant.variantcall_variantfileinfoCSV);

        rt = Runtime.getRuntime();
        System.out.println("Executing:\n\t" + sb.toString());
        //pr = rt.exec(sb.toString());

        System.out.println(">>>>> STD ERROR");
        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }

        System.out.println(">>>>> STD OUTPUT");
        in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }

        pr.waitFor();
        System.out.println("\t...done!");

        rt = Runtime.getRuntime();
        System.out.println("Starting Neo4j...");
        pr = rt.exec(neo4j + " start");
        pr.waitFor();
        System.out.println("\t...done!");

        //./neo4j stop; rm -rf ../data/databases/graph.db ; ./neo4j-admin import
        // --nodes:SAMPLE /tmp/3.vcf.sample.cvs
        // --nodes:VARIANT /tmp/3.vcf.variant.cvs
        // --nodes:VARIANT_CALL /tmp/3.vcf.variantcall.cvs
        // --nodes:VARIANT_FILE_INFO /tmp/3.vcf.variantfileinfo.cvs
        // --relationships:VARIANT_CALL /tmp/3.vcf.variant_variantcall.cvs
        // --relationships:VARIANT_CALL /tmp/3.vcf.sample_variantcall.cvs
        // --relationships:VARIANT_FILE_INFO /tmp/3.vcf.variantcall_variantfileinfo.cvs  -h ; ./neo4j start

    }

    private void updateCSVVariantFiles(List<Variant> variants, CSVforVariant csv) {
        Node node;
        PrintWriter pw;
        StringBuilder sb = new StringBuilder();
        List<String> sampleNames = null;
        if (csv.vcfHeader != null) {
            csv.vcfHeader.getSampleNamesInOrder();
        }

        for (Variant variant : variants) {
            // Variant management
            String variantId = variant.toString();
            node = NodeBuilder.newNode(0, variant);
            pw = csv.csvWriterMap.get(Node.Type.VARIANT.toString());
            pw.println(importNodeLine(variantId, node, nodeAttributes.get(Node.Type.VARIANT.toString())));

            if (ListUtils.isNotEmpty(variant.getStudies())) {
                // Only one single study is supported
                StudyEntry studyEntry = variant.getStudies().get(0);

                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                    // INFO management: FILTER, QUAL and info fields
                    String filename = studyEntry.getFiles().get(0).getFileId();
                    String infoId = variantId + "_" + filename;
                    Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();

                    sb.setLength(0);
                    Iterator<String> iterator = csv.infoSet.iterator();
                    while (iterator.hasNext()) {
                        String infoName = iterator.next();
                        if (sb.length() > 0) {
                            sb.append(",");
                        }
                        if (fileAttrs.containsKey(infoName)) {
                            sb.append(fileAttrs.get(infoName).replace(",", ";"));
                        } else {
                            sb.append("-");
                        }
                    }
                    pw = csv.csvWriterMap.get(Node.Type.VARIANT_FILE_INFO.toString());
                    pw.print(infoId + "," + infoId + "," + filename);
                    if (sb.length() > 0) {
                        pw.print(",");
                        pw.println(sb.toString());
                    } else {
                        pw.println();
                    }

                    // FORMAT: GT and format attributes
                    for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
                        sb.setLength(0);
                        String sampleName = sampleNames == null ? "sample_" + i : sampleNames.get(i);
                        String formatId = variantId + "_" + sampleName;

                        sb.setLength(0);
                        iterator = csv.formatSet.iterator();
                        while (iterator.hasNext()) {
                            String formatName = iterator.next();
                            if (sb.length() > 0) {
                                sb.append(",");
                            }
                            if (studyEntry.getFormatPositions().containsKey(formatName)) {
                                sb.append(studyEntry.getSampleData(i).get(studyEntry.getFormatPositions()
                                        .get(formatName)).replace(",", ";"));
                            } else {
                                sb.append("-");
                            }
                        }
                        pw = csv.csvWriterMap.get(Node.Type.VARIANT_CALL.toString());
                        pw.print(formatId + "," + formatId);
                        if (sb.length() > 0) {
                            pw.print(",");
                            pw.println(sb.toString());
                        } else {
                            pw.println();
                        }

                        // Relation: variant - variant call
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT__VARIANT_CALL.toString());
                        pw.println(importRelationNode(variantId, formatId));

                        // Relation: sample - variant call
                        pw = csv.csvWriterMap.get(Relation.Type.SAMPLE__VARIANT_CALL.toString());
                        pw.println(importRelationNode(sampleName, formatId));

                        // Relation: variant call - variant file info
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString());
                        pw.println(importRelationNode(formatId, infoId));
                    }
                }
            }

            // Annotation management
            if (variant.getAnnotation() != null) {
                // Consequence types
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                    // Consequence type nodes
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        String ctId = "ct_" + (uid++);
                        node = NodeBuilder.newNode(0, ct);
                        pw = csv.csvWriterMap.get(Node.Type.CONSEQUENCE_TYPE.toString());
                        pw.println(importNodeLine(ctId, node,
                                nodeAttributes.get(Node.Type.CONSEQUENCE_TYPE.toString())));

                        // Relation: variant - consequence type
                        sb.setLength(0);
                        sb.append(variantId).append(SEPARATOR).append(ctId);
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT__CONSEQUENCE_TYPE.toString());
                        pw.println(sb.toString());

                        // SO
                        if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                            for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                                if (!soSet.contains(so.getAccession())) {
                                    node = new Node(0, so.getAccession(), so.getName(), Node.Type.SO);
                                    pw = csv.csvWriterMap.get(Node.Type.SO.toString());
                                    pw.println(importNodeLine(so.getAccession(), node,
                                            nodeAttributes.get(Node.Type.SO.toString())));

                                    soSet.add(so.getAccession());
                                }

                                // Relation: consequence type - so
                                sb.setLength(0);
                                sb.append(ctId).append(SEPARATOR).append(so.getAccession());
                                pw = csv.csvWriterMap.get(Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                pw.println(sb.toString());
                            }
                        }

                        // Protein annotation: substitution scores, keywords and features
                        if (ct.getProteinVariantAnnotation() != null) {
                            // Protein variant annotation node
                            String protVarAnnoId = "pva_" + (uid++);
                            node = NodeBuilder.newNode(0, ct.getProteinVariantAnnotation());
                            pw = csv.csvWriterMap.get(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString());
                            pw.println(importNodeLine(protVarAnnoId, node,
                                    nodeAttributes.get(Node.Type.PROTEIN_VARIANT_ANNOTATION.toString())));

                            // Relation: consequence type - protein variant annotation
                            sb.setLength(0);
                            sb.append(ctId).append(SEPARATOR).append(protVarAnnoId);
                            pw = csv.csvWriterMap.get(
                                    Relation.Type.CONSEQUENCE_TYPE__PROTEIN_VARIANT_ANNOTATION.toString());
                            pw.println(sb.toString());

                            // Protein relationship management
                            String proteinId = ct.getProteinVariantAnnotation().getUniprotAccession();
                            if (proteinId != null) {
                                if (!proteinSet.contains(proteinId)) {
                                    String uniprotName = ct.getProteinVariantAnnotation().getUniprotName();
                                    node = new Node(0, proteinId, uniprotName, Node.Type.PROTEIN);
                                    pw = csv.csvWriterMap.get(Node.Type.PROTEIN.toString());
                                    pw.println(importNodeLine(proteinId, node,
                                            nodeAttributes.get(Node.Type.PROTEIN.toString())));

                                    proteinSet.add(proteinId);
                                }

                                // Relation: protein - protein variant annotation
                                sb.setLength(0);
                                sb.append(proteinId).append(SEPARATOR).append(protVarAnnoId);
                                pw = csv.csvWriterMap.get(
                                        Relation.Type.PROTEIN__PROTEIN_VARIANT_ANNOTATION.toString());
                                pw.println(sb.toString());
                            }

                            // Protein substitution scores
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                                for (Score score : ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                    String substId = "subst_" + (uid++);
                                    node = NodeBuilder.newNode(0, score, Node.Type.SUBSTITUTION_SCORE);
                                    pw = csv.csvWriterMap.get(Node.Type.SUBSTITUTION_SCORE.toString());
                                    pw.println(importNodeLine(substId, node,
                                            nodeAttributes.get(Node.Type.SUBSTITUTION_SCORE.toString())));

                                    // Relation: protein variant annotation - substitution score
                                    sb.setLength(0);
                                    sb.append(protVarAnnoId).append(SEPARATOR).append(substId);
                                    pw = csv.csvWriterMap.get(
                                            Relation.Type.PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE.toString());
                                    pw.println(sb.toString());
                                }
                            }

                            // Protein keywords
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getKeywords())) {
                                for (String keyword : ct.getProteinVariantAnnotation().getKeywords()) {
                                    if (!kwSet.contains(keyword)) {
                                        node = new Node(0, keyword, keyword, Node.Type.PROTEIN_KEYWORD);
                                        pw = csv.csvWriterMap.get(Node.Type.PROTEIN_KEYWORD.toString());
                                        pw.println(importNodeLine(keyword, node,
                                                nodeAttributes.get(Node.Type.PROTEIN_KEYWORD.toString())));

                                        kwSet.add(keyword);
                                    }

                                    // Relation: protein variant annotation - keyword
                                    sb.setLength(0);
                                    sb.append(protVarAnnoId).append(SEPARATOR).append(keyword);
                                    pw = csv.csvWriterMap.get(
                                            Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD.toString());
                                    pw.println(sb.toString());
                                }
                            }

                            // Protein features
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getFeatures())) {
                                for (ProteinFeature feature : ct.getProteinVariantAnnotation().getFeatures()) {
                                    String featId = null;
                                    if (feature.getId() != null) {
                                        featId = feature.getId();
                                    } else {
                                        featId = feature.getDescription();
                                        if (featId != null) {
                                            featId = cleanString(featId);
                                        }
                                    }
                                    if (featId != null) {
                                        if (!featureSet.contains(featId)) {
                                            node = NodeBuilder.newNode(0, feature);
                                            pw = csv.csvWriterMap.get(Node.Type.PROTEIN_FEATURE.toString());
                                            pw.println(importNodeLine(featId, node,
                                                    nodeAttributes.get(Node.Type.PROTEIN_FEATURE.toString())));

                                            featureSet.add(featId);
                                        }

                                        // Relation: protein variant annotation - keyword
                                        sb.setLength(0);
                                        sb.append(protVarAnnoId).append(SEPARATOR).append(featId);
                                        pw = csv.csvWriterMap.get(
                                                Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN_FEATURE.toString());
                                        pw.println(sb.toString());
                                    }
                                }
                            }
                        }

                        // Transcript
                        String transcriptId = ct.getEnsemblTranscriptId();
                        if (transcriptId != null) {
                            if (!transcriptSet.contains(transcriptId)) {
                                node = new Node(0, transcriptId, null, Node.Type.TRANSCRIPT);
                                pw = csv.csvWriterMap.get(Node.Type.TRANSCRIPT.toString());
                                pw.println(importNodeLine(transcriptId, node,
                                        nodeAttributes.get(Node.Type.TRANSCRIPT.toString())));

                                transcriptSet.add(transcriptId);
                            }

                            // Relation: consequence type - transcript
                            sb.setLength(0);
                            sb.append(ctId).append(SEPARATOR).append(ct.getEnsemblTranscriptId());
                            pw = csv.csvWriterMap.get(Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                            pw.println(sb.toString());
                        }

                        // Gene
                        String geneId = ct.getGeneName();
                        if (geneId != null) {
                            if (!geneSet.contains(geneId)) {
                                node = new Node(0, ct.getEnsemblGeneId(), ct.getGeneName(), Node.Type.GENE);
                                pw = csv.csvWriterMap.get(Node.Type.GENE.toString());
                                pw.println(importNodeLine(geneId, node,
                                        nodeAttributes.get(Node.Type.GENE.toString())));

                                geneSet.add(geneId);
                            }

                            // Relation: consequence type - gene
                            sb.setLength(0);
                            sb.append(ctId).append(SEPARATOR).append(geneId);
                            pw = csv.csvWriterMap.get(Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
                            pw.println(sb.toString());
                        }
                    }
                }

                // Population frequencies
                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                    for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                        // Population frequency node
                        String popFreqId = "pop_" + (uid++);
                        node = NodeBuilder.newNode(0, popFreq);
                        pw = csv.csvWriterMap.get(Node.Type.POPULATION_FREQUENCY.toString());
                        pw.println(importNodeLine(popFreqId, node,
                                nodeAttributes.get(Node.Type.POPULATION_FREQUENCY.toString())));

                        // Relation: variant - population frequency
                        sb.setLength(0);
                        sb.append(variantId).append(SEPARATOR).append(popFreqId);
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT__POPULATION_FREQUENCY.toString());
                        pw.println(sb.toString());
                    }
                }

                // Conservation values
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                    for (Score score : variant.getAnnotation().getConservation()) {
                        // Conservation node
                        String consId = "cons_" + (uid++);
                        node = NodeBuilder.newNode(0, score, Node.Type.CONSERVATION);
                        pw = csv.csvWriterMap.get(Node.Type.CONSERVATION.toString());
                        pw.println(importNodeLine(consId, node,
                                nodeAttributes.get(Node.Type.CONSERVATION.toString())));

                        // Relation: variant - conservation
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT__CONSERVATION.toString());
                        pw.println(importRelationNode(variantId, consId));
                    }
                }

                // Drug
                if (ListUtils.isNotEmpty(variant.getAnnotation().getGeneDrugInteraction())) {
                    for (GeneDrugInteraction drug : variant.getAnnotation().getGeneDrugInteraction()) {
                        String drugId = cleanString(drug.getDrugName());
                        if (drugId != null) {
                            if (!drugSet.contains(drugId)) {
                                node = NodeBuilder.newNode(0, drug);
                                pw = csv.csvWriterMap.get(Node.Type.DRUG.toString());
                                pw.println(importNodeLine(drugId, node,
                                        nodeAttributes.get(Node.Type.DRUG.toString())));

                                drugSet.add(drugId);
                            }

                            if (drug.getGeneName() != null) {
                                if (!geneSet.contains(drug.getGeneName())) {
                                    node = new Node(0, null, drug.getGeneName(), Node.Type.GENE);
                                    pw = csv.csvWriterMap.get(Node.Type.GENE.toString());
                                    pw.println(importNodeLine(drug.getGeneName(), node,
                                            nodeAttributes.get(Node.Type.GENE.toString())));

                                    geneSet.add(drug.getGeneName());
                                }

                                // Relation: gene - drug interaction
                                pw = csv.csvWriterMap.get(Relation.Type.GENE__DRUG.toString());
                                pw.println(importRelationNode(drug.getGeneName(), drugId));
                            }
                        }
                    }
                }
//                    // Disease
//                    if (ListUtils.isNotEmpty(variant.getAnnotation().getGeneTraitAssociation())) {
//                        for (GeneTraitAssociation disease: variant.getAnnotation().getGeneTraitAssociation()) {
//                            String diseaseId = disease.getId() + "_" + disease.getName();
//                            if (diseaseId != null) {
//                                diseaseId = cleanString(diseaseId);
//                                if (!diseaseSet.contains(diseaseId)) {
//                                    node = NodeBuilder.newNode(0, disease);
//                                    pw = csv.csvWriterMap.get(Node.Type.DISEASE.toString());
//                                    pw.println(importNodeLine(diseaseId, node,
//                                            nodeAttributes.get(Node.Type.DISEASE.toString())));
//
//                                    diseaseSet.add(diseaseId);
//                                }
//
//                                // Relation: gene - disease (trait association)
//                                pw = csv.csvWriterMap.get(Relation.Type.GENE__DISEASE.toString());
//                                pw.println(importRelationNode(drug.getGeneName(), drugId));
//                            }
//                        }
//                    }
//                }
                // Trait associations
                if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                    for (EvidenceEntry evidence : variant.getAnnotation().getTraitAssociation()) {
                        // Trait association node
                        String traitId = "trait_" + (uid++);
                        node = NodeBuilder.newNode(0, evidence, Node.Type.TRAIT_ASSOCIATION);
                        pw = csv.csvWriterMap.get(Node.Type.TRAIT_ASSOCIATION.toString());
                        pw.println(importNodeLine(traitId, node,
                                nodeAttributes.get(Node.Type.TRAIT_ASSOCIATION.toString())));

                        // Relation: variant - trait association
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT__TRAIT_ASSOCIATION.toString());
                        pw.println(importRelationNode(variantId, traitId));
                    }
                }

                // Functional scores
                if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                    for (Score score : variant.getAnnotation().getFunctionalScore()) {
                        // Functional score node
                        String functId = "funct_" + (uid++);
                        node = NodeBuilder.newNode(0, score, Node.Type.FUNCTIONAL_SCORE);
                        pw = csv.csvWriterMap.get(Node.Type.FUNCTIONAL_SCORE.toString());
                        pw.println(importNodeLine(functId, node,
                                nodeAttributes.get(Node.Type.FUNCTIONAL_SCORE.toString())));

                        // Relation: variant - functional score
                        pw = csv.csvWriterMap.get(Relation.Type.VARIANT__FUNCTIONAL_SCORE.toString());
                        pw.println(importRelationNode(variantId, functId));
                    }
                }
            }
        }
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

    private String importNodeLine(String uid, Node node, List<String> attrs) {
        String value;
        StringBuilder sb = new StringBuilder();
        sb.append(cleanString(uid)).append(SEPARATOR).append(cleanString(node.getId())).append(SEPARATOR)
                .append(cleanString(node.getName()));
        for (int i = 3; i < attrs.size(); i++) {
            value = node.getAttributes().getString(attrs.get(i));
            if (StringUtils.isEmpty(value)) {
                value = "-";
            } else {
                value = cleanString(value);
            }
            sb.append(SEPARATOR).append(value);
        }

        return sb.toString();
    }

    private String importRelationNode(String startId, String endId) {
        StringBuilder sb = new StringBuilder();
        sb.append(startId).append(SEPARATOR).append(endId);
        return sb.toString();
    }

    private String cleanString(String input) {
        if (StringUtils.isNotEmpty(input)) {
            return input.replace(",", ";").replace("\"", "");
        } else {
            return null;
        }
    }

    public Set<String> getGeneSet() {
        return geneSet;
    }

    public Neo4JImporter setGeneSet(Set<String> geneSet) {
        this.geneSet = geneSet;
        return this;
    }

    public Set<String> getTranscriptSet() {
        return transcriptSet;
    }

    public Neo4JImporter setTranscriptSet(Set<String> transcriptSet) {
        this.transcriptSet = transcriptSet;
        return this;
    }

    public Set<String> getSoSet() {
        return soSet;
    }

    public Neo4JImporter setSoSet(Set<String> soSet) {
        this.soSet = soSet;
        return this;
    }

    public Set<String> getKwSet() {
        return kwSet;
    }

    public Neo4JImporter setKwSet(Set<String> kwSet) {
        this.kwSet = kwSet;
        return this;
    }

    public Set<String> getFeatureSet() {
        return featureSet;
    }

    public Neo4JImporter setFeatureSet(Set<String> featureSet) {
        this.featureSet = featureSet;
        return this;
    }

    public Set<String> getProteinSet() {
        return proteinSet;
    }

    public Neo4JImporter setProteinSet(Set<String> proteinSet) {
        this.proteinSet = proteinSet;
        return this;
    }

    public Set<String> getDrugSet() {
        return drugSet;
    }

    public Neo4JImporter setDrugSet(Set<String> drugSet) {
        this.drugSet = drugSet;
        return this;
    }
}
