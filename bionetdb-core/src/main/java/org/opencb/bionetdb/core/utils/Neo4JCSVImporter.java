package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.TranscriptTfbs;
import org.opencb.biodata.models.variant.StudyEntry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Neo4JCSVImporter {
    private static final String GENE_PREFIX = "g-";

    private CSVInfo csv;
    private Neo4JBioPAXImporter bioPAXImporter;

    protected static Logger logger;

    public Neo4JCSVImporter(CSVInfo csv) {
        this.csv = csv;
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    public void addVariantFiles(List<File> files) throws IOException {
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

    public void annotateGenes(CellBaseClient cellBaseClient) throws IOException {
        // Annotate genes
        List<String> ids = new ArrayList();
        csv.getCsvWriters().get(Node.Type.GENE.toString()).close();
        logger.info("Annotating genes...");
        Path oldPath = Paths.get(csv.getOutputPath() + "/GENE.csv");
        Path newPath = Paths.get(csv.getOutputPath() + "/GENE.csv.annotated");
        BufferedReader reader = FileUtils.newBufferedReader(oldPath);
        // Skip the header line to the new CSV file
        String line = reader.readLine();
        line = reader.readLine();
        int numGenes = 0;
        while (line != null) {
            ids.add(line.split(csv.SEPARATOR)[1]);
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
        checkTranscripts(cellBaseClient);

        // Process trancripts in order to create transcript-protein relationships
        //linkTranscriptProtein(cellBaseClient);
    }

    public void annotateProteins(CellBaseClient cellBaseClient) throws IOException {
        // Annotate proteins
        List<String> ids = new ArrayList();
        csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).close();
        logger.info("Annotating proteins...");
        Path oldPath = Paths.get(csv.getOutputPath() + "/PROTEIN.csv");
        Path newPath = Paths.get(csv.getOutputPath() + "/PROTEIN.csv.annotated");
        BufferedReader reader = FileUtils.newBufferedReader(oldPath);
        // Skip the header line to the new CSV file
        String id, line = reader.readLine();
        line = reader.readLine();
        int numProteins = 0;
        int numSkip = 0;
        int numAnnotatedProteins = 0;
        while (line != null) {
            id = line.split(csv.SEPARATOR)[2];
            numProteins++;
            if (id.contains("(") || id.contains("/")) {
                numSkip++;
                //logger.info("=======> skip id: " + id);
            } else {
                ids.add(id);
                if (ids.size() > 200) {
                    numAnnotatedProteins += annotateProteins(ids, cellBaseClient);
                    ids.clear();
                }
            }

            // Read next line
            line = reader.readLine();
        }
        if (ids.size() > 0) {
            numAnnotatedProteins += annotateProteins(ids, cellBaseClient);
        }
        reader.close();
        logger.info("Annotating proteins done!");
        logger.info("Total annotated proteins {} of {}", numAnnotatedProteins, numProteins);
        logger.info("Skipping {} proteins for invalid", numSkip);

//        // Rename GENE files
//        oldPath.toFile().delete();
//        Files.move(newPath, oldPath);
//
//        // Sanity check for TRANSCRIPTS, annotated and not!
//        checkTranscripts();
    }

    public void importCSVFiles() {
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void addJSONFile(File file, boolean first) throws IOException {
        // Reading file line by line, each line a JSON object
        BufferedReader reader;
        ObjectMapper mapper = new ObjectMapper();

        File metaFile = new File(file.getAbsoluteFile() + ".meta.json");
        if (metaFile.exists()) {
            csv.openMetadataFile(metaFile);
        }

        logger.info("Processing JSON file {}", file.getPath());
        reader = FileUtils.newBufferedReader(file.toPath());
        String line = reader.readLine();
        while (line != null) {
            Variant variant = mapper.readValue(line, Variant.class);
            processVariant(variant, first);

            // read next line
            line = reader.readLine();
        }
        reader.close();
    }

    private void processVariant(Variant variant, boolean first) {
        // Variant management
        String variantId = variant.toString();

        Long variantUid = csv.getLong(variantId);
        if (variantUid != null && !first) {
            processSampleInfo(variant);
            return;
        }

        variantUid = csv.getAndIncUid();
        csv.putLong(variantId, variantUid);

        Node node = NodeBuilder.newNode(variantUid, variant);
        PrintWriter pw = csv.getCsvWriters().get(Node.Type.VARIANT.toString());
        pw.println(csv.nodeLine(node));

        processSampleInfo(variant);

        // Annotation management
        if (variant.getAnnotation() != null) {
            // Consequence types
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                // Consequence type nodes
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    Node ctNode = NodeBuilder.newNode(csv.getAndIncUid(), ct);
                    updateCSVFiles(variantUid, ctNode, Relation.Type.VARIANT__CONSEQUENCE_TYPE.toString());

                    // Gene
                    String geneName = ct.getEnsemblGeneId(); //getGeneName();
                    if (geneName != null) {
                        Long geneUid = csv.getLong(GENE_PREFIX + geneName);
                        if (geneUid == null) {
                            node = new Node(csv.getAndIncUid(), ct.getEnsemblGeneId(), ct.getGeneName(), Node.Type.GENE);
                            // Update node CSV file
//                            updateCSVFiles(ctNode.getUid(), node, Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
                            pw = csv.getCsvWriters().get(node.getType().toString());
                            pw.println(csv.nodeLine(node));

                            csv.putLong(GENE_PREFIX + geneName, node.getUid());
//                        } else {
//                            // Relation: consequence type - gene
//                            pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
//                            pw.println(csv.relationLine(ctNode.getUid(), geneUid));
                        }
                    }

                    // Transcript
                    String transcriptId = ct.getEnsemblTranscriptId();
                    if (transcriptId != null) {
                        Long transcriptUid = csv.getLong(transcriptId);
                        if (transcriptUid == null) {
                            node = new Node(csv.getAndIncUid(), transcriptId, transcriptId, Node.Type.TRANSCRIPT);
                            updateCSVFiles(ctNode.getUid(), node, Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                            csv.putLong(transcriptId, node.getUid());
                        } else {
                            // Relation: consequence type - transcript
                            pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                            pw.println(csv.relationLine(ctNode.getUid(), transcriptUid));
                        }
                    }

                    // SO
                    if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                            String soId = so.getAccession();
                            if (soId != null) {
                                Long soUid = csv.getLong(soId);
                                if (soUid == null) {
                                    node = new Node(csv.getAndIncUid(), so.getAccession(), so.getName(), Node.Type.SO);
                                    updateCSVFiles(ctNode.getUid(), node, Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                    csv.putLong(soId, node.getUid());
                                } else {
                                    // Relation: consequence type - so
                                    pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                    pw.println(csv.relationLine(ctNode.getUid(), soUid));
                                }
                            }
                        }
                    }

                    // Protein variant annotation: substitution scores, keywords and features
                    if (ct.getProteinVariantAnnotation() != null) {
                        // Protein variant annotation node
                        Node pVANode = NodeBuilder.newNode(csv.getAndIncUid(), ct.getProteinVariantAnnotation());
                        updateCSVFiles(ctNode.getUid(), pVANode,
                                Relation.Type.CONSEQUENCE_TYPE__PROTEIN_VARIANT_ANNOTATION.toString());

                        // Protein relationship management
                        String protAcc = ct.getProteinVariantAnnotation().getUniprotAccession();
                        if (protAcc != null) {
                            Long protUid = csv.getLong(protAcc);
                            if (protUid == null) {
                                Long xrefUid = csv.getLong("x0." + protAcc);
                                if (xrefUid != null) {
//                                    logger.info("PVA, accession {} found as xref {}", protAcc, xrefUid);
                                    // Relation: protein variant annotation - protein
                                    Long xrefTargetUid = csv.getLong("x1." + protAcc);
                                    pw = csv.getCsvWriters().get(Relation.Type
                                            .PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString());
                                    pw.println(csv.relationLine(pVANode.getUid(), xrefTargetUid));
                                }
//                                String protName = ct.getProteinVariantAnnotation().getUniprotName();
//                                node = new Node(csv.getAndIncUid(), protAcc, protName, Node.Type.PROTEIN);
//                                updateCSVFiles(pVANode.getUid(), node,
//                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString());
//                                csv.putLong(protAcc, node.getUid());
                            } else {
                                // Relation: protein variant annotation - protein
                                pw = csv.getCsvWriters().get(Relation.Type
                                        .PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString());
                                pw.println(csv.relationLine(pVANode.getUid(), protUid));
                            }
                        }

                        // Protein substitution scores
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                            for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.SUBSTITUTION_SCORE);
                                updateCSVFiles(pVANode.getUid(), node,
                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE.toString());
                            }
                        }

                        // Protein keywords
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getKeywords())) {
                            for (String keyword: ct.getProteinVariantAnnotation().getKeywords()) {
                                Long kwUid = csv.getLong(keyword);
                                if (kwUid == null) {
                                    node = new Node(csv.getAndIncUid(), keyword, keyword, Node.Type.PROTEIN_KEYWORD);
                                    updateCSVFiles(pVANode.getUid(), node,
                                            Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD.toString());
                                    csv.putLong(keyword, node.getUid());
                                } else {
                                    // Relation: protein variant annotation - keyword
                                    pw = csv.getCsvWriters().get(Relation.Type
                                            .PROTEIN_VARIANT_ANNOTATION__PROTEIN_KEYWORD.toString());
                                    pw.println(csv.relationLine(pVANode.getUid(), kwUid));
                                }
                            }
                        }

                        // Protein features
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getFeatures())) {
                            for (ProteinFeature feature: ct.getProteinVariantAnnotation().getFeatures()) {
                                node = NodeBuilder.newNode(csv.getAndIncUid(), feature);
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
                    node = NodeBuilder.newNode(csv.getAndIncUid(), popFreq);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__POPULATION_FREQUENCY.toString());
                }
            }

            // Conservation values
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                for (Score score: variant.getAnnotation().getConservation()) {
                    // Conservation node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.CONSERVATION);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__CONSERVATION.toString());
                }
            }

            // Trait associations
            if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                for (EvidenceEntry evidence: variant.getAnnotation().getTraitAssociation()) {
                    // Trait association node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), evidence, Node.Type.TRAIT_ASSOCIATION);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__TRAIT_ASSOCIATION.toString());
                }
            }

            // Functional scores
            if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    // Functional score node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.FUNCTIONAL_SCORE);
                    updateCSVFiles(variantUid, node, Relation.Type.VARIANT__FUNCTIONAL_SCORE.toString());
                }
            }
        }
    }

    private void processSampleInfo(Variant variant) {
        String variantId = variant.toString();
        Long variantUid = csv.getLong(variantId);

        if (ListUtils.isNotEmpty(variant.getStudies())) {
            PrintWriter pw;

            // Only one single study is supported
            StudyEntry studyEntry = variant.getStudies().get(0);

            if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                // INFO management: FILTER, QUAL and info fields
                String filename = studyEntry.getFiles().get(0).getFileId();
                String infoId = variantId + "_" + filename;
                Long infoUid = csv.getLong(infoId);
                if (infoUid == null) {
                    infoUid = csv.getAndIncUid();
                    pw = csv.getCsvWriters().get(Node.Type.VARIANT_FILE_INFO.toString());
                    pw.print(variantInfoLine(infoUid, studyEntry));
                }

                // FORMAT: GT and format attributes
                for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
                    String sampleName = csv.getSampleNames() == null ? "sample" + (i + 1) : csv.getSampleNames().get(i);
                    Long sampleUid = csv.getLong(sampleName);
                    if (sampleUid == null) {
                        sampleUid = csv.getAndIncUid();
                        pw = csv.getCsvWriters().get(Node.Type.SAMPLE.toString());
                        pw.println(sampleUid + "," + sampleName + "," + sampleName);
                        csv.putLong(sampleName, sampleUid);
                    }
                    Long formatUid = csv.getAndIncUid();
                    pw = csv.getCsvWriters().get(Node.Type.VARIANT_CALL.toString());
                    pw.println(variantFormatLine(formatUid, studyEntry, i));

                    // Relation: variant - variant call
                    pw = csv.getCsvWriters().get(Relation.Type.VARIANT__VARIANT_CALL.toString());
                    pw.println(csv.relationLine(variantUid, formatUid));

                    // Relation: sample - variant call
                    pw = csv.getCsvWriters().get(Relation.Type.SAMPLE__VARIANT_CALL.toString());
                    pw.println(csv.relationLine(sampleUid, formatUid));

                    // Relation: variant call - variant file info
                    pw = csv.getCsvWriters().get(Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString());
                    pw.println(csv.relationLine(formatUid, infoUid));
                }
            }
        }
    }

    private String variantInfoLine(long infoUid, StudyEntry studyEntry) {
        StringBuilder sb = new StringBuilder();
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
//                    if (sb.length() > 0) {
//                        pw.print(",");
//                        pw.println(sb.toString());
//                    } else {
//                        pw.println();
//                    }
//        pw.println();
//        pw.print(infoUid + "," + infoUid + "," + filename);
        return sb.toString();
    }

    private String variantFormatLine(long formatUid, StudyEntry studyEntry, int index) {
        StringBuilder sb = new StringBuilder();

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
//                    if (sb.length() > 0) {
//                        pw.print(",");
//                        pw.println(sb.toString());
//                    } else {
//                        pw.println();
//                    }
        //formatUid + "," + formatId + "," + studyEntry.getSampleData(i).get(0)
        return sb.toString();
    }

    private void updateCSVFiles(long startUid, Node node, String relationType) {
        updateCSVFiles(startUid, node, relationType, false);
    }

    private void updateCSVFiles(long startUid, Node node, String relationType, boolean annotated) {
        // Update node CSV file
        PrintWriter pw = annotated
                ? csv.getCsvAnnotatedWriters().get(node.getType().toString())
                : csv.getCsvWriters().get(node.getType().toString());
        pw.println(csv.nodeLine(node));

        // Update relation CSV file
        pw = csv.getCsvWriters().get(relationType);
        pw.println(csv.relationLine(startUid, node.getUid()));
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
//                Long geneUid = csv.getLong(GENE_PREFIX + queryResult.getId());
//                Node node = new Node(geneUid, queryResult.getId(), null, Node.Type.GENE);
//                csv.getCsvAnnotatedWriters().get(Node.Type.GENE.toString()).println(csv.nodeLine(node));
            }
        }
    }

    private void processGene(Gene gene) {
        PrintWriter pw;
        // Gene node
        long geneUid;
        try {
            geneUid = csv.getLong(GENE_PREFIX + gene.getId());
//          geneUid = rocksDBManager.getLong(GENE_PREFIX + gene.getName(), rocksDB);
            Node node = NodeBuilder.newNode(geneUid, gene);
            pw = csv.getCsvAnnotatedWriters().get(node.getType().toString());
            pw.println(csv.nodeLine(node));
        } catch (Exception e) {
            logger.warn("Internal error gene {}, {} is missing in database", gene.getId(), gene.getName());
            return;
            //e.printStackTrace();
        }

        // Transcripts
        if (ListUtils.isNotEmpty(gene.getTranscripts())) {
//            System.out.println(gene.getId());
            for (Transcript transcript: gene.getTranscripts()) {
//                System.out.println("\t" + transcript.getId() + ", " + transcript.getName());
                Long transcriptUid = csv.getLong(transcript.getId());
                Long aTranscriptUid = csv.getLong("a" + transcript.getId());
                if (aTranscriptUid == null) {
                    // Create new UID for the transcript and insert it into the rocksdb as annotated transcript
                    // Take the valid UID and insert it into the rocksdb as annotated transcript
                    aTranscriptUid = transcriptUid == null ? csv.getAndIncUid() : transcriptUid;
                    Node node = NodeBuilder.newNode(aTranscriptUid, transcript);
                    updateCSVFiles(geneUid, node, Relation.Type.GENE__TRANSCRIPT.toString(), true);
                    csv.putLong("a" + transcript.getId(), node.getUid());
                    csv.putString(geneUid + "." + aTranscriptUid, "1");
                }

                // Check gene-transcript relation and create it if it does not exist
                String relGeneTrans = csv.getString(geneUid + "." + aTranscriptUid);
                if (relGeneTrans == null) {
                    // Relation: gene - transcript
                    pw = csv.getCsvWriters().get(Relation.Type.GENE__TRANSCRIPT.toString());
                    pw.println(csv.relationLine(geneUid, aTranscriptUid));
                    csv.putString(geneUid + "." + aTranscriptUid, "1");
                }


//                // Protein
//                if (transcript.getProteinID() != null) {
//                    Node node =  new Node();
//                }

                // Tfbs
                if (ListUtils.isNotEmpty(transcript.getTfbs())) {
                    for (TranscriptTfbs tfbs: transcript.getTfbs()) {
                        Node node = NodeBuilder.newNode(csv.getAndIncUid(), tfbs);
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
                    Long drugUid = csv.getLong(drug.getDrugName());
                    if (drugUid == null) {
                        Node node = NodeBuilder.newNode(csv.getAndIncUid(), drug);
                        updateCSVFiles(geneUid, node, Relation.Type.GENE__DRUG.toString());

                        csv.putLong(drug.getDrugName(), node.getUid());
                    } else {
                        // Relation: gene - drug
                        pw = csv.getCsvWriters().get(Relation.Type.GENE__DRUG.toString());
                        pw.println(csv.relationLine(geneUid, drugUid));
                    }
                }

                // Disease
                if (ListUtils.isNotEmpty(gene.getAnnotation().getDiseases())) {
                    for (GeneTraitAssociation disease : gene.getAnnotation().getDiseases()) {
                        String diseaseId = disease.getId() + "_" + (disease.getHpo() != null ? disease.getHpo() : "");
                        Long diseaseUid = csv.getLong(diseaseId);
                        if (diseaseUid == null) {
                            Node node = NodeBuilder.newNode(csv.getAndIncUid(), disease);
                            updateCSVFiles(geneUid, node, Relation.Type.GENE__DISEASE.toString());

                            csv.putLong(diseaseId, node.getUid());
                        } else {
                            // Relation: gene - disease
                            pw = csv.getCsvWriters().get(Relation.Type.GENE__DISEASE.toString());
                            pw.println(csv.relationLine(geneUid, diseaseUid));
                        }
                    }
                }
            }
        }
    }

    private int annotateProteins(List<String> ids, CellBaseClient cellBaseClient) throws IOException {
        int counter = 0;
        List<String> notFound = new ArrayList<>();
        QueryOptions options = new QueryOptions("EXCLUDE", "reference,comment,sequence,evidence");
        QueryResponse<Entry> response = cellBaseClient.getProteinClient().get(ids, options);
        for (QueryResult<Entry> result: response.getResponse()) {
            if (ListUtils.isNotEmpty(result.getResult())) {
                counter += result.getResult().size();
                for (Entry protein: result.getResult()) {
                    processProtein(protein, result.getId());
                }
//            } else {
                // This should not happen, but...
//                notFound.add(result.getId());
                //Query query = new Query(ProteinDBAdaptor.QueryParams.XREFS.toString(), "");

//                QueryResponse<Entry> response = cellBaseClient.getProteinClient().search(query, options);
//                logger.info("CellBase does not found results for query {}, try later", result.getId());
//                Long geneUid = csv.getLong(GENE_PREFIX + result.getId());
//                Node node = new Node(geneUid, queryResult.getId(), null, Node.Type.GENE);
//                csv.getCsvAnnotatedWriters().get(Node.Type.GENE.toString()).println(csv.nodeLine(node));
            }
        }
//        Query query = new Query(ProteinDBAdaptor.QueryParams.XREFS.toString(),
//                StringUtils.join(notFound, ","));
//        response = cellBaseClient.getProteinClient().search(query, options);
//        for (QueryResult<Entry> result: response.getResponse()) {
//            if (ListUtils.isNotEmpty(result.getResult())) {
//                counter += result.getResult().size();
//                for (Entry protein: result.getResult()) {
//                    logger.info("!!!! Second chance and successfull for query {}", result.getId());
//                    processProtein(protein, result.getId());
//                }
//            } else {
//                // This should not happen, but...
//                logger.info("Second chance, and CellBase does not found results for query {}", result.getId());
//            }
//        }

        return counter;
    }

    private void processProtein(Entry protein, String id) {
        PrintWriter pw;
        Long protUid = csv.getLong(id);
        if (protUid != null) {
            for (String acc: protein.getAccession()) {
                Long xrefUid = csv.getLong("x0." + acc);
                if (xrefUid == null) {
                    Node node = new Node(csv.getAndIncUid(), acc, acc, Node.Type.XREF);
                    node.addAttribute("dbName", "uniprot");
                    pw = csv.getCsvWriters().get(node.getType().toString());
                    pw.println(csv.nodeLine(node));

                    xrefUid = node.getUid();
                    csv.putLong("x0." + acc, xrefUid);
                }
                Long xrefTargetUid = csv.getLong("x1." + acc);
                if (xrefTargetUid == null) {
                    pw = csv.getCsvWriters().get(CSVInfo.BioPAXRelation.XREF___PROTEIN___XREF.toString());
                    pw.println(csv.relationLine(protUid, xrefUid));
                    csv.putLong("x1." + acc, protUid);
                } else {
                    if (xrefTargetUid != protUid) {
                        logger.info("!!!! XREF {} points these proteins {}, {}", acc, protUid, xrefTargetUid);
                    }
                }

            }
        } else {
            logger.error("Protein {} is not stored in database", id);
        }
    }

    private int processTranscripts(List<String> ids, CellBaseClient cellBaseClient) throws IOException {
        int counter = 0;
        PrintWriter pw;
        //Query query = new Query(ProteinDBAdaptor.QueryParams.XREFS.toString(), ids);
        QueryOptions options = new QueryOptions("exclude", "reference,comment,sequence,evidence");
        logger.info("Calling CellBase to get protein IDs from {} transcripts...", ids.size());
        QueryResponse<Entry> response = cellBaseClient.getProteinClient().get(ids, options);
        logger.info("\t...done!");
        for (QueryResult<Entry> result : response.getResponse()) {
            if (ListUtils.isNotEmpty(result.getResult())) {
                counter += result.getResult().size();
                String transcriptId = result.getId();
                Long transcriptUid = csv.getLong(transcriptId);
                if (transcriptUid != null) {
                    for (Entry protein : result.getResult()) {
                        if (ListUtils.isNotEmpty(protein.getAccession())) {
                            for (String acc : protein.getAccession()) {
                                Long uid = csv.getLong(acc);
                                if (uid != null) {
                                    // This is the protein itself
                                    if (csv.getLong(transcriptUid + "." + uid) == null) {
                                        pw = csv.getCsvWriters().get(Relation.Type.TRANSCRIPT__PROTEIN.toString());
                                        pw.println(csv.relationLine(transcriptUid, uid));
                                        csv.putLong(transcriptUid + "." + uid, 1);
                                    }
                                } else {
                                    uid = csv.getLong("x1." + acc);
                                    if (uid != null) {
                                        if (csv.getLong(transcriptUid + "." + uid) == null) {
                                            // This is a XREF to the protein
                                            pw = csv.getCsvWriters().get(Relation.Type.TRANSCRIPT__PROTEIN.toString());
                                            pw.println(csv.relationLine(transcriptUid, uid));
                                            csv.putLong(transcriptUid + "." + uid, 1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    logger.info("Transcript {} is not stored in the datahase", transcriptId);
                }
            }
        }
        return counter;
    }

    private void checkTranscripts(CellBaseClient cellBaseClient) throws IOException {
        csv.getCsvWriters().get(Node.Type.TRANSCRIPT.toString()).close();

        List<String> ids = new ArrayList<>();

        Path oldPath = Paths.get(csv.getOutputPath() + "/TRANSCRIPT.csv");
        BufferedReader reader = FileUtils.newBufferedReader(oldPath);
        String line = reader.readLine();
        int numLine = 1;
        line = reader.readLine();
        ++numLine;
        while (line != null) {
            String id = line.split(",")[1];
            if (csv.getLong("a" + id) == null) {
                csv.getCsvAnnotatedWriters().get(Node.Type.TRANSCRIPT.toString()).println(line);
            }

            // Process ids
            ids.add(id);
            if (ids.size() > 200) {
                processTranscripts(ids, cellBaseClient);
                ids.clear();
            }

            line = reader.readLine();
        }
        reader.close();

        // Remaining ids to process
        if (ids.size() > 0) {
            processTranscripts(ids, cellBaseClient);
        }

        Path newPath = Paths.get(csv.getOutputPath() + "/TRANSCRIPT.csv.annotated");
        oldPath.toFile().delete();
        Files.move(newPath, oldPath);
    }
}
