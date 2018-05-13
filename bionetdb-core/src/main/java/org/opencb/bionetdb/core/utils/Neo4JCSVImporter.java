package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.TranscriptTfbs;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Neo4JCSVImporter {
    private static final String SEPARATOR = ",";
    private static final String MISSING_VALUE = "-";
    private static final String GENE_PREFIX = "g-";
    private static final String PROTEIN_PREFIX = "p-";

    private Path outputPath;

    private Map<String, PrintWriter> csvWriters;
    private Map<String, PrintWriter> csvAnnotatedWriters;
    private Map<String, List<String>> nodeAttributes;

    private Set<String> formatFields;
    private Set<String> infoFields;

    private RocksDBManager rocksDBManager;
    private RocksDB rocksDB;

    private long uid;

    protected static Logger logger;

    public Neo4JCSVImporter(Path outputPath) {
        this.uid = 0;

        this.outputPath = outputPath;
        this.logger = LoggerFactory.getLogger(this.getClass());

        this.csvWriters = new HashMap<>();
        this.csvAnnotatedWriters = new HashMap<>();
        this.nodeAttributes = createNodeAttributes();

        this.rocksDBManager = new RocksDBManager();
        this.rocksDB = this.rocksDBManager.getDBConnection(outputPath.toString() + "/rocksdb", true);

        try {
            openCSVFiles();
        } catch (IOException e) {
            e.printStackTrace();
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

    public void addVariantFiles(List<File> files) throws IOException {
        //openCSVFiles(files, true);

        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.getName().endsWith("vcf") || file.getName().endsWith("vcf.gz")) {
                // VCF file
                logger.error("VCF not supported yet!!");
            } else if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
                // JSON file
                addJSONFile(file, (i == 0));
            } else {
                logger.error("VCF not supported yet!!");
            }
        }
    }

    public void addReactomeFiles(List<File> files) {

    }

    public void annotate(CellBaseClient cellBaseClient) throws IOException {
        // Annotate genes
        List<String> ids = new ArrayList();
        csvWriters.get(Node.Type.GENE.toString()).close();
        logger.info("Annotating genes...");
        Path oldPath = Paths.get(outputPath + "/GENE.csv");
        Path newPath = Paths.get(outputPath + "/GENE.csv.annotated");
        BufferedReader reader = FileUtils.newBufferedReader(oldPath);
        // Skip the header line to the new CSV file
        String line = reader.readLine();
        line = reader.readLine();
        int numGenes = 0;
        while (line != null) {
            ids.add(line.split(SEPARATOR)[1]);
            if (ids.size() > 200) {
                annotateGenes(ids, cellBaseClient);
                numGenes += ids.size();
                logger.info("\tTotal annotated genes: " + numGenes);
                ids.clear();
            }

            // Read next line
            line = reader.readLine();
        }
        if (ids.size() > 0) {
            annotateGenes(ids, cellBaseClient);

            numGenes += ids.size();
            logger.info("\tTotal annotated genes: " + numGenes);
        }
        reader.close();
        logger.info("Annotating genes done!");

        // Rename GENE files
        oldPath.toFile().delete();
        Files.move(newPath, oldPath);

        // Sanity check for TRANSCRIPTS, annotated and not!
        checkTranscripts();
    }

    public void importCSVFiles() {
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void openCSVFiles() throws IOException {
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
        for (String type: types) {
            filename = type + ".csv.annotated";
            pw = new PrintWriter(outputPath + "/" + filename);
            csvAnnotatedWriters.put(type, pw);
            if (ListUtils.isNotEmpty(nodeAttributes.get(type))) {
                pw.println(getNodeHeaderLine(nodeAttributes.get(type)));
            }
        }

        // CSV files for relationships
        for (Relation.Type type: Relation.Type.values()) {
            if (type.toString().contains("__")) {
                filename = type.toString() + ".csv";
//                pw = new PrintWriter(new BufferedWriter(new FileWriter(outputPath + "/" + filename, !header)));
                pw = new PrintWriter(outputPath + "/" + filename);
                csvWriters.put(type.toString(), pw);

                pw.println(getRelationHeaderLine(type.toString()));
            }
        }
    }

    private void addJSONFile(File file, boolean save) throws IOException {
        // Reading file line by line, each line a JSON object
        BufferedReader reader;
        ObjectMapper mapper = new ObjectMapper();

        logger.info("Processing JSON file {}", file.getPath());
        reader = FileUtils.newBufferedReader(file.toPath());
        String line = reader.readLine();
        while (line != null) {
            Variant variant = mapper.readValue(line, Variant.class);
            processVariant(variant, save);

            // read next line
            line = reader.readLine();
        }
        reader.close();
    }

    private void processVariant(Variant variant, boolean save) {
        // Variant management
        String variantId = variant.toString();

        Long variantUid = rocksDBManager.getLong(variantId, rocksDB);
        if (variantUid != null && !save) {
            return;
        }

        variantUid = ++uid;
        rocksDBManager.putLong(variantId, variantUid, rocksDB);

        Node node = NodeBuilder.newNode(variantUid, variant);
        PrintWriter pw = csvWriters.get(Node.Type.VARIANT.toString());
        pw.println(nodeLine(node));

//        if (ListUtils.isNotEmpty(variant.getStudies())) {
//            // Only one single study is supported
//            StudyEntry studyEntry = variant.getStudies().get(0);
//
//            if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
//                // INFO management: FILTER, QUAL and info fields
//                String filename = studyEntry.getFiles().get(0).getFileId();
//                String infoId = variantId + "_" + filename;
//                Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();
//
//                sb.setLength(0);
//                Iterator<String> iterator = csv.infoSet.iterator();
//                while (iterator.hasNext()) {
//                    String infoName = iterator.next();
//                    if (sb.length() > 0) {
//                        sb.append(",");
//                    }
//                    if (fileAttrs.containsKey(infoName)) {
//                        sb.append(fileAttrs.get(infoName).replace(",", ";"));
//                    } else {
//                        sb.append("-");
//                    }
//                }
//                pw = csv.csvWriterMap.get(Node.Type.VARIANT_FILE_INFO.toString());
//                pw.print(infoId + "," + infoId + "," + filename);
//                if (sb.length() > 0) {
//                    pw.print(",");
//                    pw.println(sb.toString());
//                } else {
//                    pw.println();
//                }
//
//                // FORMAT: GT and format attributes
//                for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
//                    sb.setLength(0);
//                    String sampleName = sampleNames == null ? "sample_" + i : sampleNames.get(i);
//                    String formatId = variantId + "_" + sampleName;
//
//                    sb.setLength(0);
//                    iterator = csv.formatSet.iterator();
//                    while (iterator.hasNext()) {
//                        String formatName = iterator.next();
//                        if (sb.length() > 0) {
//                            sb.append(",");
//                        }
//                        if (studyEntry.getFormatPositions().containsKey(formatName)) {
//                            sb.append(studyEntry.getSampleData(i).get(studyEntry.getFormatPositions()
//                                    .get(formatName)).replace(",", ";"));
//                        } else {
//                            sb.append("-");
//                        }
//                    }
//                    pw = csv.csvWriterMap.get(Node.Type.VARIANT_CALL.toString());
//                    pw.print(formatId + "," + formatId);
//                    if (sb.length() > 0) {
//                        pw.print(",");
//                        pw.println(sb.toString());
//                    } else {
//                        pw.println();
//                    }
//
//                    // Relation: variant - variant call
//                    pw = csv.csvWriterMap.get(Relation.Type.VARIANT__VARIANT_CALL.toString());
//                    pw.println(importRelationNode(variantId, formatId));
//
//                    // Relation: sample - variant call
//                    pw = csv.csvWriterMap.get(Relation.Type.SAMPLE__VARIANT_CALL.toString());
//                    pw.println(importRelationNode(sampleName, formatId));
//
//                    // Relation: variant call - variant file info
//                    pw = csv.csvWriterMap.get(Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString());
//                    pw.println(importRelationNode(formatId, infoId));
//                }
//            }
//        }

        // Annotation management
        if (variant.getAnnotation() != null) {
            // Consequence types
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                // Consequence type nodes
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    Node ctNode = NodeBuilder.newNode(++uid, ct);
                    updateCSVFiles(variantUid, ctNode, Relation.Type.VARIANT__CONSEQUENCE_TYPE.toString());

                    // Gene
                    String geneName = ct.getEnsemblGeneId(); //getGeneName();
                    if (geneName != null) {
                        Long geneUid = rocksDBManager.getLong(GENE_PREFIX + geneName, rocksDB);
                        if (geneUid == null) {
                            node = new Node(++uid, ct.getEnsemblGeneId(), ct.getGeneName(), Node.Type.GENE);
                            updateCSVFiles(ctNode.getUid(), node, Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
                            rocksDBManager.putLong(GENE_PREFIX + geneName, node.getUid(), rocksDB);
                        } else {
                            // Relation: consequence type - gene
                            pw = csvWriters.get(Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
                            pw.println(relationLine(ctNode.getUid(), geneUid));
                        }
                    }

                    // Transcript
                    String transcriptId = ct.getEnsemblTranscriptId();
                    if (transcriptId != null) {
                        Long transcriptUid = rocksDBManager.getLong(transcriptId, rocksDB);
                        if (transcriptUid == null) {
                            node = new Node(++uid, transcriptId, transcriptId, Node.Type.TRANSCRIPT);
                            updateCSVFiles(ctNode.getUid(), node, Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                            rocksDBManager.putLong(transcriptId, node.getUid(), rocksDB);
                        } else {
                            // Relation: consequence type - transcript
                            pw = csvWriters.get(Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                            pw.println(relationLine(ctNode.getUid(), transcriptUid));
                        }
                    }

                    // SO
                    if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                            String soId = so.getAccession();
                            if (soId != null) {
                                Long soUid = rocksDBManager.getLong(soId, rocksDB);
                                if (soUid == null) {
                                    node = new Node(++uid, so.getAccession(), so.getName(), Node.Type.SO);
                                    updateCSVFiles(ctNode.getUid(), node, Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                    rocksDBManager.putLong(soId, node.getUid(), rocksDB);
                                } else {
                                    // Relation: consequence type - so
                                    pw = csvWriters.get(Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                    pw.println(relationLine(ctNode.getUid(), soUid));
                                }
                            }
                        }
                    }

                    // Protein variant annotation: substitution scores, keywords and features
                    if (ct.getProteinVariantAnnotation() != null) {
                        // Protein variant annotation node
                        Node pVANode = NodeBuilder.newNode(++uid, ct.getProteinVariantAnnotation());
                        updateCSVFiles(ctNode.getUid(), pVANode,
                                Relation.Type.CONSEQUENCE_TYPE__PROTEIN_VARIANT_ANNOTATION.toString());

                        // Protein relationship management
                        String protAcc = ct.getProteinVariantAnnotation().getUniprotAccession();
                        if (protAcc != null) {
                            Long protUid = rocksDBManager.getLong(protAcc, rocksDB);
                            if (protUid == null) {
                                String protName = ct.getProteinVariantAnnotation().getUniprotName();
                                node = new Node(++uid, protAcc, protName, Node.Type.PROTEIN);
                                updateCSVFiles(pVANode.getUid(), node,
                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString());
                                rocksDBManager.putLong(protAcc, node.getUid(), rocksDB);
                            } else {
                                // Relation: protein variant annotation - protein
                                pw = csvWriters.get(Relation.Type
                                        .PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString());
                                pw.println(relationLine(pVANode.getUid(), protUid));
                            }

                        }

                        // Protein substitution scores
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                            for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                node = NodeBuilder.newNode(++uid, score, Node.Type.SUBSTITUTION_SCORE);
                                updateCSVFiles(pVANode.getUid(), node,
                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE.toString());
                            }
                        }

                        // Protein keywords
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getKeywords())) {
                            for (String keyword: ct.getProteinVariantAnnotation().getKeywords()) {
                                Long kwUid = rocksDBManager.getLong(keyword, rocksDB);
                                if (kwUid == null) {
                                    node = new Node(++uid, keyword, keyword, Node.Type.PROTEIN_KEYWORD);
                                    updateCSVFiles(pVANode.getUid(), node,
                                            Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD.toString());
                                    rocksDBManager.putLong(keyword, node.getUid(), rocksDB);
                                } else {
                                    // Relation: protein variant annotation - keyword
                                    pw = csvWriters.get(Relation.Type
                                            .PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD.toString());
                                    pw.println(relationLine(pVANode.getUid(), kwUid));
                                }
                            }
                        }

                        // Protein features
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getFeatures())) {
                            for (ProteinFeature feature: ct.getProteinVariantAnnotation().getFeatures()) {
                                node = NodeBuilder.newNode(++uid, feature);
                                updateCSVFiles(pVANode.getUid(), node,
                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN_FEATURE.toString());
                            }
                        }
                    }
                }
            }

            // Population frequencies
            if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                    // Population frequency node
                    node = NodeBuilder.newNode(++uid, popFreq);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__POPULATION_FREQUENCY.toString());
                }
            }

            // Conservation values
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                for (Score score: variant.getAnnotation().getConservation()) {
                    // Conservation node
                    node = NodeBuilder.newNode(++uid, score, Node.Type.CONSERVATION);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__CONSERVATION.toString());
                }
            }

            // Trait associations
            if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                for (EvidenceEntry evidence: variant.getAnnotation().getTraitAssociation()) {
                    // Trait association node
                    node = NodeBuilder.newNode(++uid, evidence, Node.Type.TRAIT_ASSOCIATION);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__TRAIT_ASSOCIATION.toString());
                }
            }

            // Functional scores
            if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    // Functional score node
                    node = NodeBuilder.newNode(++uid, score, Node.Type.FUNCTIONAL_SCORE);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__FUNCTIONAL_SCORE.toString());
                }
            }
        }
    }

    private void updateCSVFiles(long startUid, Node node, String relationType) {
        updateCSVFiles(startUid, node, relationType, false);
    }

    private void updateCSVFiles(long startUid, Node node, String relationType, boolean annotated) {
        // Update node CSV file
        PrintWriter pw = annotated
                ? csvAnnotatedWriters.get(node.getType().toString())
                : csvWriters.get(node.getType().toString());
        pw.println(nodeLine(node));

        // Update relation CSV file
        pw = csvWriters.get(relationType);
        pw.println(relationLine(startUid, node.getUid()));
    }

    private void annotateGenes(List<String> ids, CellBaseClient cellBaseClient) throws IOException {
        QueryOptions options = new QueryOptions("EXCLUDE", "transcripts.exons,transcripts.cDnaSequence");
        QueryResponse<Gene> entryQueryResponse = cellBaseClient.getGeneClient().get(ids, options);
        for (QueryResult<Gene> queryResult: entryQueryResponse.getResponse()) {
            if (ListUtils.isNotEmpty(queryResult.getResult())) {
                for (Gene gene: queryResult.getResult()) {
                    processGene(gene);
                }
            } else {
                // This should not happen, but...
                logger.error("CellBase does not found results for query {}", queryResult.toString());
                Long geneUid = rocksDBManager.getLong(GENE_PREFIX + queryResult.getId(), rocksDB);
                Node node = new Node(geneUid, queryResult.getId(), null, Node.Type.GENE);
                csvAnnotatedWriters.get(Node.Type.GENE.toString()).println(nodeLine(node));
            }
        }
    }

    private void processGene(Gene gene) {
        PrintWriter pw;
        // Gene node
        long geneUid;
        try {
            geneUid = rocksDBManager.getLong(GENE_PREFIX + gene.getId(), rocksDB);
//          geneUid = rocksDBManager.getLong(GENE_PREFIX + gene.getName(), rocksDB);
            Node node = NodeBuilder.newNode(geneUid, gene);
            pw = csvAnnotatedWriters.get(node.getType().toString());
            pw.println(nodeLine(node));
        } catch (Exception e) {
            logger.warn("Internal error gene {}, {} is missing in database", gene.getId(), gene.getName());
            return;
            //e.printStackTrace();
        }

        // Transcripts
        if (ListUtils.isNotEmpty(gene.getTranscripts())) {
            for (Transcript transcript: gene.getTranscripts()) {
                Long transcriptUid = rocksDBManager.getLong(transcript.getId(), rocksDB);
                Long aTranscriptUid = rocksDBManager.getLong("a" + transcript.getId(), rocksDB);
                if (aTranscriptUid == null) {
                    // Create new UID for the transcript and insert it into the rocksdb as annotated transcript
                    // Take the valid UID and insert it into the rocksdb as annotated transcript
                    aTranscriptUid = transcriptUid == null ? ++uid : transcriptUid;
                    Node node = NodeBuilder.newNode(aTranscriptUid, transcript);
                    updateCSVFiles(geneUid, node, Relation.Type.GENE__TRANSCRIPT.toString(), true);
                    rocksDBManager.putLong("a" + transcript.getId(), node.getUid(), rocksDB);
                }

                // Check gene-transcript relation and create it if it does not exist
                String relGeneTrans = rocksDBManager.getString(geneUid + "." + aTranscriptUid, rocksDB);
                if (relGeneTrans == null) {
                    // Relation: gene - transcript
                    pw = csvWriters.get(Relation.Type.GENE__TRANSCRIPT.toString());
                    pw.println(relationLine(geneUid, aTranscriptUid));
                    rocksDBManager.putString(geneUid + "." + aTranscriptUid, "1", rocksDB);
                }

//                // Protein
//                if (transcript.getProteinID() != null) {
//                    Node node =  new Node();
//                }

                // Tfbs
                if (ListUtils.isNotEmpty(transcript.getTfbs())) {
                    for (TranscriptTfbs tfbs: transcript.getTfbs()) {
                        Node node = NodeBuilder.newNode(++uid, tfbs);
                        updateCSVFiles(aTranscriptUid, node, Relation.Type.TRANSCRIPT__TFBS.toString());
                    }
                }

//                // Xrefs
//                if (ListUtils.isNotEmpty(transcript.getXrefs())) {
//                    for (org.opencb.biodata.models.core.Xref xref: transcript.getXrefs()) {
//                        Node xrefNode = NodeBuilder.newNode(uidCounter, xref);
//                        networkDBAdaptor.mergeNode(xrefNode, "id", "dbName", tx);
//
//                        // Relation: transcript - xref
//                        Relation tXrefRel = new Relation(++uidCounter, null, tNode.getUid(), tNode.getType(), xrefNode.getUid(),
//                                xrefNode.getType(), Relation.Type.XREF);
//                        networkDBAdaptor.mergeRelation(tXrefRel, tx);
//                    }
//                }
            }
        }

        if (gene.getAnnotation() != null) {
            // Drug
            if (ListUtils.isNotEmpty(gene.getAnnotation().getDrugs())) {
                for (GeneDrugInteraction drug : gene.getAnnotation().getDrugs()) {
                    Long drugUid = rocksDBManager.getLong(drug.getDrugName(), rocksDB);
                    if (drugUid == null) {
                        Node node = NodeBuilder.newNode(++uid, drug);
                        updateCSVFiles(geneUid, node, Relation.Type.GENE__DRUG.toString());

                        rocksDBManager.putLong(drug.getDrugName(), node.getUid(), rocksDB);
                    } else {
                        // Relation: gene - drug
                        pw = csvWriters.get(Relation.Type.GENE__DRUG.toString());
                        pw.println(relationLine(geneUid, drugUid));
                    }
                }

                // Disease
                if (ListUtils.isNotEmpty(gene.getAnnotation().getDiseases())) {
                    for (GeneTraitAssociation disease : gene.getAnnotation().getDiseases()) {
                        String diseaseId = disease.getId() + "_" + (disease.getHpo() != null ? disease.getHpo() : "");
                        Long diseaseUid = rocksDBManager.getLong(diseaseId, rocksDB);
                        if (diseaseUid == null) {
                            Node node = NodeBuilder.newNode(++uid, disease);
                            updateCSVFiles(geneUid, node, Relation.Type.GENE__DISEASE.toString());

                            rocksDBManager.putLong(diseaseId, node.getUid(), rocksDB);
                        } else {
                            // Relation: gene - disease
                            pw = csvWriters.get(Relation.Type.GENE__DISEASE.toString());
                            pw.println(relationLine(geneUid, diseaseUid));
                        }
                    }
                }
            }
        }
    }

    private void checkTranscripts() throws IOException {
        csvWriters.get(Node.Type.TRANSCRIPT.toString()).close();

        Path oldPath = Paths.get(outputPath + "/TRANSCRIPT.csv");
        BufferedReader reader = FileUtils.newBufferedReader(oldPath);
        String line = reader.readLine();
        int numLine = 1;
        line = reader.readLine();
        ++numLine;
        while (line != null) {
            String id = line.split(",")[1];
            if (rocksDBManager.getLong("a" + id, rocksDB) == null) {
                csvAnnotatedWriters.get(Node.Type.TRANSCRIPT.toString()).println(line);
            }
            line = reader.readLine();
        }
        reader.close();

        Path newPath = Paths.get(outputPath + "/TRANSCRIPT.csv.annotated");
        oldPath.toFile().delete();
        Files.move(newPath, oldPath);
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

    private String nodeLine(Node node) {
        List<String> attrs = nodeAttributes.get(node.getType().toString());

        StringBuilder sb = new StringBuilder();
        sb.append(node.getUid()).append(SEPARATOR);
        String value = cleanString(node.getId());
        sb.append(StringUtils.isEmpty(value) ? MISSING_VALUE : value).append(SEPARATOR);
        value = cleanString(node.getName());
        sb.append(StringUtils.isEmpty(value) ? MISSING_VALUE : value);
        for (int i = 3; i < attrs.size(); i++) {
            value = cleanString(node.getAttributes().getString(attrs.get(i)));
            sb.append(SEPARATOR).append(StringUtils.isEmpty(value) ? MISSING_VALUE : value);
        }

        return sb.toString();
    }

    private String relationLine(long startUid, long endUid) {
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

        return nodeAttributes;
    }
}
