package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.DbReferenceType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.FeatureType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.KeywordType;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.TranscriptTfbs;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Neo4jCsvImporter {
    public static final Object GENE_FILENAME = "genes.json";
    public static final Object GENE_DBNAME = "genes.rocksdb";

    public static final Object PROTEIN_FILENAME = "proteins.json";
    public static final Object PROTEIN_DBNAME = "proteins.rocksdb";

    public static final Object MIRNA_FILENAME = "mirna.csv";
    public static final Object MIRNA_DBNAME = "mirna.rocksdb";

    private CsvInfo csv;

    protected static Logger logger;

    public Neo4jCsvImporter(CsvInfo csv) {
        this.csv = csv;

        logger = LoggerFactory.getLogger(this.getClass());
    }

    public void addVariantFiles(List<File> files) throws IOException {
        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
                if (file.getName().contains("clinvar")) {
                    // JSON file
                    addJSONFile(file);
                }
            }
        }


        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.getName().endsWith("vcf") || file.getName().endsWith("vcf.gz")) {
                // VCF file
                logger.info("VCF not supported yet!!");
            } else if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
                if (!file.getName().contains("clinvar") && !file.getName().contains("meta.json")) {
                    // JSON file
                    addJSONFile(file);
                }
            } else {
                logger.info("Unknown file type, skipping!!");
            }
        }
    }

    public Long processGene(String geneId, String geneName) {
        Long geneUid = null;
        if (StringUtils.isEmpty(geneId) && StringUtils.isEmpty(geneName)) {
            return geneUid;
        }
        Gene gene = csv.getGene(geneId);
        if (gene != null) {
            geneUid = csv.getLong(gene.getId());
            if (geneUid == null) {
                // Create gene node and write the CSV file
                Node node = createGeneNode(gene);
                csv.getCsvWriters().get(Node.Type.GENE.toString()).println(csv.nodeLine(node));
                geneUid = node.getUid();

                // Save gene UID
                saveGeneUid(gene, geneUid);
            }
        }
        return geneUid;
    }

    public Long processProtein(String proteinId, String proteinName) {
        Long proteinUid = null;
        if (StringUtils.isEmpty(proteinId) && StringUtils.isEmpty(proteinName)) {
            return proteinUid;
        }
        Entry protein = csv.getProtein(proteinId);
        if (protein != null) {
            Node node;
            proteinUid = csv.getLong(proteinId);
            if (proteinUid == null) {
                // Create gene node and write the CSV file
                node = createProteinNode(protein);
                csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).println(csv.nodeLine(node));
                // Save protein UID
                saveProteinUid(protein, node.getUid());
            } else {
                node = new Node(csv.getAndIncUid(), proteinId, proteinName, Node.Type.PROTEIN);
                // Save CSV node
                csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).println(csv.nodeLine(node));
                // Save protein UID
                if (StringUtils.isNotEmpty(proteinId)) {
                    csv.putLong(proteinId, proteinUid);
                }
                if (StringUtils.isNotEmpty(proteinName)) {
                    csv.putLong(proteinName, proteinUid);
                }
            }
        }

        return proteinUid;
    }

    public Long processTranscript(String transcriptId) {
        if (StringUtils.isEmpty(transcriptId)) {
            return null;
        }
        return csv.getLong(transcriptId);
    }

    public Node createGeneNode(Gene gene) {
        return createGeneNode(gene, csv.getAndIncUid());
    }

    public Node createGeneNode(Gene gene, Long uid) {
        PrintWriter pwRel;

        // Create gene node
        Node node = NodeBuilder.newNode(uid, gene);

        // Model transcripts
        if (ListUtils.isNotEmpty(gene.getTranscripts())) {
            pwRel = csv.getCsvWriters().get(Relation.Type.GENE__TRANSCRIPT.toString());
            PrintWriter pwTranscr = csv.getCsvWriters().get(Node.Type.TRANSCRIPT.toString());
            for (Transcript transcript: gene.getTranscripts()) {
                Long transcrUid = csv.getLong(transcript.getId());
                if (transcrUid == null) {
                    // Create gene node and write the CSV file
                    Node transcrNode = createTranscriptNode(transcript);
                    pwTranscr.println(csv.nodeLine(transcrNode));

                    // Save the gene node uid
                    transcrUid = transcrNode.getUid();
                    csv.putLong(transcript.getId(), transcrUid);
                }

                // Write gene-transcript relation
                pwRel.println(uid + "," + transcrUid);
            }
        }

        Node n;

        // Model gene annotation: drugs, diseases,...
        if (gene.getAnnotation() != null) {
            // Model drugs
            if (ListUtils.isNotEmpty(gene.getAnnotation().getDrugs())) {
                pwRel = csv.getCsvWriters().get(Relation.Type.GENE__DRUG.toString());
                for (GeneDrugInteraction drug: gene.getAnnotation().getDrugs()) {
                    Long drugUid = csv.getLong(drug.getDrugName());
                    if (drugUid == null) {
                        n = NodeBuilder.newNode(csv.getAndIncUid(), drug);
                        updateCSVFiles(uid, n, Relation.Type.GENE__DRUG.toString());
                        csv.putLong(drug.getDrugName(), n.getUid());
                    } else {
                        // Create gene-drug relation
                        pwRel.println(csv.relationLine(uid, drugUid));
                    }
                }

                // Model diseases
                if (ListUtils.isNotEmpty(gene.getAnnotation().getDiseases())) {
                    pwRel = csv.getCsvWriters().get(Relation.Type.GENE__DISEASE.toString());
                    for (GeneTraitAssociation disease : gene.getAnnotation().getDiseases()) {
                        String diseaseId = disease.getId() + "_" + (disease.getHpo() != null ? disease.getHpo() : "");
                        Long diseaseUid = csv.getLong(diseaseId);
                        if (diseaseUid == null) {
                            n = NodeBuilder.newNode(csv.getAndIncUid(), disease);
                            updateCSVFiles(uid, node, Relation.Type.GENE__DISEASE.toString());

                            csv.putLong(diseaseId, n.getUid());
                        } else {
                            // create gene-disease relation
                            pwRel.println(csv.relationLine(uid, diseaseUid));
                        }
                    }
                }
            }
        }

        return node;
    }

    public Node completeProteinNode(Node node) {
        Entry protein = csv.getProtein(node.getName());
        if (protein == null) {
            return node;
        }

        // Create protein node and add the attributes from the previous node
        Node newNode = createProteinNode(protein, node.getUid());
        for (String key: node.getAttributes().keySet()) {
            newNode.addAttribute(key, node.getAttributes().get(key));
        }
        return newNode;
    }

    public Node createProteinNode(Entry protein) {
        return createProteinNode(protein, csv.getAndIncUid());
    }

    public Node createProteinNode(Entry protein, Long uid) {
        Node n;
        PrintWriter pw;

        // Create protein node
        Node node = NodeBuilder.newNode(uid, protein);

        // Model protein keywords
        if (ListUtils.isNotEmpty(protein.getKeyword())) {
            pw = csv.getCsvWriters().get(Relation.Type.PROTEIN__PROTEIN_KEYWORD.toString());
            for (KeywordType keyword: protein.getKeyword()) {
                Long kwUid = csv.getLong(keyword.getId());
                if (kwUid == null) {
                    n = new Node(csv.getAndIncUid(), keyword.getId(), keyword.getValue(), Node.Type.PROTEIN_KEYWORD);
                    updateCSVFiles(uid, n, Relation.Type.PROTEIN__PROTEIN_KEYWORD.toString());
                    csv.putLong(keyword.getId(), n.getUid());
                } else {
                    // Create protein - protein keyword relation
                    pw.println(csv.relationLine(uid, kwUid));
                }
            }
        }

        // Model protein features
        if (ListUtils.isNotEmpty(protein.getFeature())) {
            for (FeatureType feature: protein.getFeature()) {
                n = NodeBuilder.newNode(csv.getAndIncUid(), feature);
                updateCSVFiles(uid, n, Relation.Type.PROTEIN__PROTEIN_FEATURE.toString());
            }
        }

        // Model Xrefs
        PrintWriter pwXref = csv.getCsvWriters().get(Node.Type.XREF.toString());
        pw = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.XREF___PROTEIN___XREF.toString());
        if (ListUtils.isNotEmpty(protein.getDbReference())) {
            for (DbReferenceType dbRef: protein.getDbReference()) {
                n = NodeBuilder.newNode(csv.getAndIncUid(), dbRef);
                pwXref.println(csv.nodeLine(n));
                pw.println(csv.relationLine(uid, n.getUid()));
            }
        }

        // Save protein UID
        saveProteinUid(protein, uid);

        // Return node
        return node;
    }

    public Node createTranscriptNode(Transcript transcript) {
        return createTranscriptNode(transcript, csv.getAndIncUid());
    }

    public Node createTranscriptNode(Transcript transcript, Long uid) {
        // Create transcript node
        Node node = NodeBuilder.newNode(uid, transcript);

        // Get protein
        Node n;
        if (ListUtils.isNotEmpty(transcript.getXrefs())) {
            Entry protein = null;
            for (org.opencb.biodata.models.core.Xref xref: transcript.getXrefs()) {
                protein = csv.getProtein(xref.getId());
                if (protein != null) {
                    break;
                }
            }
            if (protein != null) {
                PrintWriter pw;
                Long proteinUid = csv.getLong(protein.getAccession().get(0));
                if (proteinUid == null) {
                    // Create protein node and write the CSV file
                    Node proteinNode = createProteinNode(protein);
                    csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).println(csv.nodeLine(proteinNode));
                    proteinUid = proteinNode.getUid();
                }

                // Write transcript-protein relation
                csv.getCsvWriters().get(Relation.Type.TRANSCRIPT__PROTEIN.toString()).println(uid + "," + proteinUid);
            }
        }

        // TFBS
        if (ListUtils.isNotEmpty(transcript.getTfbs())) {
            for (TranscriptTfbs tfbs: transcript.getTfbs()) {
                n = NodeBuilder.newNode(csv.getAndIncUid(), tfbs);
                updateCSVFiles(uid, n, Relation.Type.TRANSCRIPT__TFBS.toString());
            }
        }

        return node;
    }

    public Long processVariant(Variant variant) {
        Long variantUid = csv.getLong(variant.toString());
        if (variantUid == null) {
            Node node = createVariantNode(variant);
            variantUid = node.getUid();
            csv.putLong(variant.toString(), variantUid);
        }

        // Process sample info
        processSampleInfo(variant, variantUid);

        return variantUid;
    }

    public Node createVariantNode(Variant variant) {
        return createVariantNode(variant, csv.getAndIncUid());
    }

    public Node createVariantNode(Variant variant, Long varUid) {
        Node n, node = NodeBuilder.newNode(varUid, variant);
        PrintWriter pw = csv.getCsvWriters().get(Node.Type.VARIANT.toString());
        pw.println(csv.nodeLine(node));

        // Annotation management
        if (variant.getAnnotation() != null) {
            // Consequence types

            if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                // Consequence type nodes
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    Node ctNode = NodeBuilder.newNode(csv.getAndIncUid(), ct);
                    updateCSVFiles(varUid, ctNode, Relation.Type.VARIANT__CONSEQUENCE_TYPE.toString());

//                    // Gene
//                    Long geneUid = processGene(ct.getEnsemblGeneId(), ct.getGeneName());
//                    if (geneUid != null) {
//                        // Relation: consequence type - gene
//                        pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
//                        pw.println(csv.relationLine(ctNode.getUid(), geneUid));
//                    }

                    // Transcript
                    Long transcriptUid = processTranscript(ct.getEnsemblTranscriptId());
                    if (transcriptUid != null) {
//                        if (geneUid != null && csv.getLong(geneUid + "." + transcriptUid) == null) {
//                            csv.getCsvWriters().get(Relation.Type.GENE__TRANSCRIPT.toString()).println(csv.relationLine(
//                                    geneUid, transcriptUid));
//                            csv.putLong(geneUid + "." + transcriptUid, 1);
//                        }

                        // Relation: consequence type - transcript
                        pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                        pw.println(csv.relationLine(ctNode.getUid(), transcriptUid));
//                    } else {
//                        if (geneUid != null) {
//                            logger.info("Transcript UID is null for {} (gene {}), maybe version mismatch!!"
//                                            + " Gene vs variant annotation!!!", ct.getEnsemblTranscriptId(),
//                                    ct.getEnsemblGeneId());
//                        }
                    }

                    // SO
                    if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                            String soId = so.getAccession();
                            if (soId != null) {
                                Long soUid = csv.getLong(soId);
                                if (soUid == null) {
                                    n = new Node(csv.getAndIncUid(), so.getAccession(), so.getName(), Node.Type.SO);
                                    updateCSVFiles(ctNode.getUid(), n, Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                    csv.putLong(soId, n.getUid());
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
                        Long protUid = processProtein(ct.getProteinVariantAnnotation().getUniprotAccession(),
                                ct.getProteinVariantAnnotation().getUniprotName());
                        if (protUid != null) {
                            // Relation: protein variant annotation - protein
                            pw = csv.getCsvWriters().get(Relation.Type.PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString());
                            pw.println(csv.relationLine(pVANode.getUid(), protUid));
                        }

                        // Protein substitution scores
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                            for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.SUBSTITUTION_SCORE);
                                updateCSVFiles(pVANode.getUid(), node,
                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE.toString());
                            }
                        }
                    }
                }
            }

            // Population frequencies
            if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                    // Population frequency node
                    n = NodeBuilder.newNode(csv.getAndIncUid(), popFreq);
                    updateCSVFiles(varUid, n, Relation.Type.VARIANT__POPULATION_FREQUENCY.toString());
                }
            }

            // Conservation values
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                for (Score score: variant.getAnnotation().getConservation()) {
                    // Conservation node
                    n = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.CONSERVATION);
                    updateCSVFiles(varUid, n, Relation.Type.VARIANT__CONSERVATION.toString());
                }
            }

            // Trait associations
            if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                for (EvidenceEntry evidence: variant.getAnnotation().getTraitAssociation()) {
                    // Trait association node
                    n = NodeBuilder.newNode(csv.getAndIncUid(), evidence, Node.Type.TRAIT_ASSOCIATION);
                    updateCSVFiles(varUid, n, Relation.Type.VARIANT__TRAIT_ASSOCIATION.toString());
                }
            }

            // Functional scores
            if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    // Functional score node
                    n = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.FUNCTIONAL_SCORE);
                    updateCSVFiles(varUid, n, Relation.Type.VARIANT__FUNCTIONAL_SCORE.toString());
                }
            }
        }

        return node;
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void saveGeneUid(Gene gene, Long uid) {
        csv.putLong(gene.getId(), uid);
    }

    private void saveProteinUid(Entry protein, Long uid) {
        if (ListUtils.isNotEmpty(protein.getAccession())) {
            for (String key: protein.getAccession()) {
                csv.putLong(key, uid);
            }
        }
        if (ListUtils.isNotEmpty(protein.getName())) {
            for (String key: protein.getName()) {
                csv.putLong(key, uid);
            }
        }
    }

    private void addJSONFile(File file) throws IOException {
        // Reading file line by line, each line a JSON object
        BufferedReader reader;
        ObjectMapper mapper = new ObjectMapper();

        File metaFile = new File(file.getAbsoluteFile() + ".meta.json");
        if (metaFile.exists()) {
            csv.openMetadataFile(metaFile);
        } else {
            metaFile = new File(file.getAbsoluteFile() + ".meta.json.gz");
            if (metaFile.exists()) {
                csv.openMetadataFile(metaFile);
            }
        }

        long counter = 0;
        logger.info("Processing JSON file {}", file.getPath());
        reader = FileUtils.newBufferedReader(file.toPath());
        String line = reader.readLine();
        while (line != null) {
            Variant variant = mapper.readValue(line, Variant.class);
            processVariant(variant);

            // read next line
            line = reader.readLine();
            if (++counter % 5000 == 0) {
                logger.info("Parsing {} variants...", counter);
            }
        }
        reader.close();
        logger.info("Parsed {} variants from {}. Done!!!", counter, file.toString());
    }

    private void processSampleInfo(Variant variant, Long variantUid) {
        String variantId = variant.toString();

        if (ListUtils.isNotEmpty(variant.getStudies())) {
            PrintWriter pw;

            // Only one single study is supported
            StudyEntry studyEntry = variant.getStudies().get(0);

            if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                // INFO management: FILTER, QUAL and info fields
                String fileId = studyEntry.getFiles().get(0).getFileId();
                String infoId = variantId + "_" + fileId;
                Long infoUid = csv.getLong(infoId);
                if (infoUid == null) {
                    // Variant file info node
                    infoUid = csv.getAndIncUid();
                    pw = csv.getCsvWriters().get(Node.Type.VARIANT_FILE_INFO.toString());
                    pw.println(variantInfoLine(infoUid, studyEntry));
                }
                Long fileUid = csv.getLong(fileId);
                if (fileUid != null) {
                    pw = csv.getCsvWriters().get(Relation.Type.VARIANT_FILE_INFO__FILE.toString());
                    pw.println(infoUid + "," + fileUid);
                }

                // FORMAT: GT and format attributes
                for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
                    String sampleName = ListUtils.isEmpty(csv.getSampleNames())
                            ? "sample" + (i + 1)
                            : csv.getSampleNames().get(i);
                    Long sampleUid = csv.getLong(sampleName);
                    if (sampleUid == null) {
                        sampleUid = csv.getAndIncUid();
                        csv.getCsvWriters().get(Node.Type.SAMPLE.toString())
                                .println(sampleUid + "," + sampleName + "," + sampleName);
                        csv.putLong(sampleName, sampleUid);
                    }
                    // Variant call node
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
        StringBuilder sb = new StringBuilder("" + infoUid);
        Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();

        Iterator<String> iterator = csv.getInfoFields().iterator();
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
        return sb.toString();
    }

    private String variantFormatLine(Long formatUid, StudyEntry studyEntry, int index) {
        StringBuilder sb = new StringBuilder("" + formatUid);

        Iterator<String> iterator = csv.getFormatFields().iterator();
        while (iterator.hasNext()) {
            String formatName = iterator.next();
            if (sb.length() > 0) {
                sb.append(",");
            }
            if (studyEntry.getFormatPositions().containsKey(formatName)) {
                sb.append(studyEntry.getSampleData(index).get(studyEntry.getFormatPositions()
                        .get(formatName)).replace(",", ";"));
            } else {
                sb.append("-");
            }
        }
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

    public void indexingGenes(Path genePath, Path indexPath) throws IOException {
        csv.indexingGenes(genePath, indexPath);
    }

    public void indexingProteins(Path proteinPath, Path indexPath) throws IOException {
        csv.indexingProteins(proteinPath, indexPath);
    }

    public void indexingMiRnas(Path proteinPath, Path indexPath) throws IOException {
        csv.indexingMiRnas(proteinPath, indexPath);
    }

    public CsvInfo getCsvInfo() {
        return csv;
    }
}
