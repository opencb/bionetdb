package org.opencb.bionetdb.lib.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v202003jaxb.*;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.*;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.metadata.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.opencb.bionetdb.lib.db.Neo4jBioPaxBuilder;
import org.opencb.bionetdb.lib.utils.cache.GeneCache;
import org.opencb.bionetdb.lib.utils.cache.ProteinCache;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Builder {
    public static final Object ENSEMBL_GENE_FILENAME = "gene.json";
    public static final Object REFSEQ_GENE_FILENAME = "refseq.json";
    public static final Object GENE_DBNAME = "gene.rocksdb";

    public static final Object PANEL_FILENAME = "panels.json";

    public static final Object PROTEIN_FILENAME = "protein.json";
    public static final Object PROTEIN_DBNAME = "protein.rocksdb";

    public static final Object MIRNA_FILENAME = "mirna.csv";
    public static final Object MIRNA_DBNAME = "mirna.rocksdb";

    public static final Object NETWORK_FILENAME = "Homo_sapiens.owl";

    public static final Object CLINICAL_VARIANT_FILENAME = "clinical_variants.full.json";

    private List<String> additionalVariantFiles;
    private List<String> additionalNeworkFiles;

    private CsvInfo csv;
    private Path inputPath;
    private Path outputPath;
    private Map<String, Set<String>> filters;
    private ObjectMapper mapper;

    private List<String> sampleIds;

    protected static Logger logger;

    public Builder(Path inputPath, Path outputPath, Map<String, Set<String>> filters) {

        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.filters = filters;

        // Prepare CSV object
        csv = new CsvInfo(inputPath, outputPath);

        // Prepare jackson writer (object to string)
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        this.logger = LoggerFactory.getLogger(this.getClass().toString());
    }

    public void build() throws IOException {
        long start;

        // Check input files

        File ensemblGeneFile = new File(inputPath + "/" + ENSEMBL_GENE_FILENAME);
        if (!ensemblGeneFile.exists()) {
            ensemblGeneFile = new File(inputPath + "/" + ENSEMBL_GENE_FILENAME + ".gz");
        }

        File refSeqGeneFile = new File(inputPath + "/" + REFSEQ_GENE_FILENAME);
        if (!refSeqGeneFile.exists()) {
            refSeqGeneFile = new File(inputPath + "/" + REFSEQ_GENE_FILENAME + ".gz");
        }

        if (!ensemblGeneFile.exists() && !refSeqGeneFile.exists()) {
            throw new IOException("Missing Ensembl/RefSeq gene files");
        }

        File proteinFile = new File(inputPath + "/" + PROTEIN_FILENAME);
        if (!proteinFile.exists()) {
            proteinFile = new File(inputPath + "/" + PROTEIN_FILENAME + ".gz");
            FileUtils.checkFile(proteinFile.toPath());
        }

        File panelFile = new File(inputPath + "/" + PANEL_FILENAME);
        FileUtils.checkFile(panelFile.toPath());

        File networkFile = new File(inputPath + "/" + NETWORK_FILENAME);
        FileUtils.checkFile(networkFile.toPath());

        File clinicalVariantFile = new File(inputPath + "/" + CLINICAL_VARIANT_FILENAME);
        if (!clinicalVariantFile.exists()) {
            clinicalVariantFile = new File(inputPath + "/" + CLINICAL_VARIANT_FILENAME + ".gz");
            FileUtils.checkFile(clinicalVariantFile.toPath());
        }

        // Group the variant files before openning the CSV files
        List<File> variantFiles = new ArrayList<>();
        variantFiles.add(clinicalVariantFile);
        if (CollectionUtils.isNotEmpty(additionalVariantFiles)) {
            for (String additionalVariantFile : additionalVariantFiles) {
                File file = Paths.get(additionalVariantFile).toFile();
                if (file.exists()) {
                    variantFiles.add(file);
                } else {
                    System.out.println("Ignoring variant file " + additionalVariantFile + ": it does not exist.");
                }
            }
        }

        // Create and open CSV files
        csv.openCSVFiles(variantFiles);

        // Processing metadata files
        logger.info("Processing metadata files...");
        start = System.currentTimeMillis();
        processMetadata(variantFiles);
        logger.info("Metadata processing done in {} s", (System.currentTimeMillis() - start) / 1000);

        // Processing proteins
        logger.info("Processing proteins...");
        start = System.currentTimeMillis();
        buildProteins(proteinFile.toPath());
        logger.info("Protein processing done in {} s", (System.currentTimeMillis() - start) / 1000);

        // Processing genes
        if (ensemblGeneFile.exists()) {
            logger.info("Processing Ensembl genes...");
            start = System.currentTimeMillis();
            buildGenes(ensemblGeneFile.toPath());
            logger.info("Ensembl gene processing done in {} s", (System.currentTimeMillis() - start) / 1000);
        }

        if (refSeqGeneFile.exists()) {
            logger.info("Processing RefSeq genes...");
            start = System.currentTimeMillis();
            buildGenes(refSeqGeneFile.toPath());
            logger.info("RefSeq gene processing done in {} s", (System.currentTimeMillis() - start) / 1000);
        }

        // Disease panels support
        logger.info("Processing disease panels...");
        start = System.currentTimeMillis();
        buildDiseasePanels(panelFile.toPath());
        logger.info("Disease panels processing done in {} s", (System.currentTimeMillis() - start) / 1000);

        // Procesing BioPAX file
        BioPAXProcessing biopaxProcessing = new BioPAXProcessing(this);
        Neo4jBioPaxBuilder bioPAXImporter = new Neo4jBioPaxBuilder(csv, filters, biopaxProcessing);
        start = System.currentTimeMillis();
        bioPAXImporter.build(networkFile.toPath());
        biopaxProcessing.post();
        logger.info("Processing BioPax/reactome file done in {} s", (System.currentTimeMillis() - start) / 1000);

        // Processing additional variants
        if (CollectionUtils.isNotEmpty(additionalVariantFiles)) {
            for (String additionalVariantFile: additionalVariantFiles) {
                // Read sample IDs
                sampleIds = readSampleIds(additionalVariantFile);

                logger.info("Processing additional variant file {}...", additionalVariantFile);
                start = System.currentTimeMillis();
                buildVariants(Paths.get(additionalVariantFile));
                logger.info("Processing additional variant file done in {} s", (System.currentTimeMillis() - start) / 1000);
            }
        }

        // Processing clinical variants
        logger.info("Processing clinical variants...");
        start = System.currentTimeMillis();
        buildVariants(clinicalVariantFile.toPath());
        logger.info("Processing clinical variants done in {} s", (System.currentTimeMillis() - start) / 1000);

        // Processing additional networks
        if (CollectionUtils.isNotEmpty(additionalNeworkFiles)) {
            for (String additionalNeworkFile: additionalNeworkFiles) {
                logger.info("Processing additional network file {}...", additionalNeworkFile);
                start = System.currentTimeMillis();
                processAdditionalNetwork(additionalNeworkFile);
                logger.info("Processing additional network file done in {} s", (System.currentTimeMillis() - start) / 1000);
            }
        }

        // Close CSV files
        csv.close();
    }

    //-------------------------------------------------------------------------
    //  BioPAX importer callback object
    //-------------------------------------------------------------------------

    public class BioPAXProcessing implements Neo4jBioPaxBuilder.BioPAXProcessing {
        private Builder builder;

        private List<Node> dnaNodes;
        private List<Node> rnaNodes;


        public BioPAXProcessing(Builder builder) {
            this.builder = builder;
            dnaNodes = new ArrayList<>();
            rnaNodes = new ArrayList<>();
        }

        public void post() {
            CsvInfo csv = builder.getCsvInfo();
            PrintWriter pwNode, pwRel;

            // Post-process DNA nodes
            logger.info("Post-processing {} dna nodes", dnaNodes.size());
            pwNode = csv.getCsvWriters().get(Node.Type.DNA.toString());
            pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.IS___DNA___GENE.toString());
            for (Node node: dnaNodes) {
                List<String> geneIds = getGeneIds(node.getName());
                for (String geneId: geneIds) {
                    Long geneUid = builder.processGene(geneId, geneId);
                    if (geneUid != null) {
                        // Write dna-gene relation
                        pwRel.println(node.getUid() + CsvInfo.SEPARATOR + geneUid);
                    }
                }
                // Write DNA node
                pwNode.println(csv.nodeLine(node));
            }

            // Post-process RNA nodes
            logger.info("Post-processing {} rna nodes", rnaNodes.size());
            pwNode = csv.getCsvWriters().get(Node.Type.RNA.toString());
            pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.IS___RNA___MIRNA.toString());
            for (Node node: rnaNodes) {
                // Write RNA node
                pwNode.println(csv.nodeLine(node));

                if (StringUtils.isNotEmpty(node.getName())) {
                    if (node.getName().startsWith("miR")) {
                        String id = "hsa-" + node.getName().toLowerCase();
                        Long miRnaUid = csv.getLong(id, Node.Type.MIRNA.name());
                        if (miRnaUid != null) {
                            // Write relation rna - mirna
                            pwRel.println(csv.relationLine(node.getUid(), miRnaUid));
                        }
                    }
                }
            }
        }

        @Override
        public void processNodes(List<Node> nodes) {
            PrintWriter pw;
            for (Node node: nodes) {
                pw = builder.getCsvInfo().getCsvWriters().get(node.getType().toString());

                if (StringUtils.isNotEmpty(node.getName())) {
                    if (node.getType() == Node.Type.PROTEIN) {
                        // Complete node proteins
                        node = builder.completeProteinNode(node);
                    } else if (node.getType() == Node.Type.DNA) {
                        // Save save gene nodes to process further, in the post-processing phase
                        dnaNodes.add(node);
                        continue;
                    } else if (node.getType() == Node.Type.RNA) {
                        // Save save miRNA nodes to process further, in the post-processing phase
                        rnaNodes.add(node);
                        continue;
                    }
                }

                // Write node to CSV file
                pw.println(builder.getCsvInfo().nodeLine(node));
            }
        }

        @Override
        public void processRelations(List<Relation> relations) {
            PrintWriter pw;
            for (Relation relation: relations) {
                String id = relation.getType()
                        + Neo4jBioPaxBuilder.BioPAXProcessing.SEPARATOR + relation.getOrigType()
                        + Neo4jBioPaxBuilder.BioPAXProcessing.SEPARATOR + relation.getDestType();
                pw = builder.getCsvInfo().getCsvWriters().get(id);
                if (pw == null) {
                    logger.info("BioPAX relationship not yet supported {}", id);
                } else {
                    // Write relation to CSV file
                    pw.println(builder.getCsvInfo().relationLine(relation.getOrigUid(), relation.getDestUid()));
                }
            }
        }

        private List<String> getGeneIds(String nodeName) {
            List<String> ids = new ArrayList<>();
            if (nodeName.toLowerCase().contains("genes") || nodeName.contains("(") || nodeName.contains(",")
                    || nodeName.contains(";")) {
                return ids;
            } else {
                String name = nodeName.replace("gene", "").replace("Gene", "").trim();
                ids.add(name);
                return ids;
            }
        }

        private List<String> getMiRnaInfo(String nodeName) {
            String[] sufixes0 = {"", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
            String[] sufixes1 = {"-3p", "-5p"};
            List<String> list = new ArrayList<>();
            if (nodeName.contains("miR")) {
                for (String s0: sufixes0) {
                    for (String s1: sufixes1) {
                        String id = "hsa-" + nodeName + s0 + s1;
                        String info = builder.getCsvInfo().getMiRnaInfo(id);
                        if (info != null) {
                            list.add(info);
                        }
                    }
                }
            }
            return list;
        }
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

//    private void addVariantFiles(List<File> files) throws IOException {
//        for (int i = 0; i < files.size(); i++) {
//            File file = files.get(i);
//            if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
//                if (file.getName().contains("clinvar")) {
//                    // JSON file
//                    addVariantJsonFile(file);
//                }
//            }
//        }
//
//
//        for (int i = 0; i < files.size(); i++) {
//            File file = files.get(i);
//            if (file.getName().endsWith("vcf") || file.getName().endsWith("vcf.gz")) {
//                // VCF file
//                logger.info("VCF not supported yet!!");
//            } else if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
//                if (!file.getName().contains("clinvar") && !file.getName().contains("meta.json")) {
//                    // JSON file
//                    addVariantJsonFile(file);
//                }
//            } else {
//                logger.info("Unknown file type, skipping!!");
//            }
//        }
//    }

//    public void addClinicalAnalysisFiles(List<File> files) throws IOException {
//        for (int i = 0; i < files.size(); i++) {
//            File file = files.get(i);
//            if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
//                if (file.getName().contains("clinvar")) {
//                    // JSON file
//                    addVariantJsonFile(file);
//                }
//            }
//        }
//
//
//        for (int i = 0; i < files.size(); i++) {
//            File file = files.get(i);
//            if (file.getName().endsWith("json") || file.getName().endsWith("json.gz")) {
//                if (!file.getName().contains("clinvar") && !file.getName().contains("meta.json")) {
//                    // JSON file
//                    addClinicalAnalysisJsonFile(file);
//                }
//            } else {
//                logger.info("Unknown file type, skipping (only JSON format are accepted for Clinical Analysis data!!");
//            }
//        }
//    }

    private Long processGene(String geneId, String geneName) {
        if (StringUtils.isEmpty(geneId)) {
            logger.info("Skip processing gene, (id, name) = (" + geneId + ", " + geneName + ")");
            return null;
        }
        Long geneUid = csv.getGeneUid(geneId);
        if (geneUid == null) {
            Node node;
            Gene gene = csv.getGene(geneId);
            if (gene != null) {
                // Create gene node
                node = createGeneNode(gene);
                // Save gene UID
                csv.saveGeneUid(geneId, node.getUid());
            } else {
                // Special case, gene not annotated!
                logger.info("Processing gene {}, {}, unknown!!", geneId, geneName);
                node = new Node(csv.getAndIncUid(), geneId, geneName, Node.Type.GENE);
                // Save gene UID
                csv.saveUnknownGeneUid(geneId, geneName, node.getUid());
            }
            // Write gene node into the CSV file
            csv.getCsvWriters().get(Node.Type.GENE.toString()).println(csv.nodeLine(node));
            return node.getUid();
        } else {
            return geneUid;
        }
    }

    private Long processProtein(String proteinId, String proteinName) {
        if (StringUtils.isEmpty(proteinId)) {
            return null;
        }
        Long proteinUid = csv.getProteinUid(proteinId);

        if (proteinUid == null) {
            Node node;
            Entry protein = csv.getProtein(proteinId);
            if (protein != null) {
                // Create protein node
                node = createProteinNode(protein);
                // Save protein UID
                csv.saveProteinUid(proteinId, node.getUid());
            } else {
                // Special case, protein not annotated!
                logger.info("Processing protein {}, {}, unknown!!", proteinId, proteinName);
                node = new Node(csv.getAndIncUid(), proteinId, proteinName, Node.Type.PROTEIN);
                // Save protein UID
                csv.saveUnknownProteinUid(proteinId, proteinName, node.getUid());
            }
            // Write protein node into the CSV file
            csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).println(csv.nodeLine(node));
            proteinUid = node.getUid();
        }

        return proteinUid;
    }

    private Long processTranscript(String transcriptId) {
        if (StringUtils.isEmpty(transcriptId)) {
            return null;
        }
        return csv.getLong(transcriptId,  Node.Type.TRANSCRIPT.name());
    }

    private Node createGeneNode(Gene gene) {
        return createGeneNode(gene, csv.getAndIncUid());
    }

    private Node createGeneNode(Gene gene, Long uid) {
        Node n;
        PrintWriter pwRel;

//        logger.info("----> creating gene node: " + gene.getId() + ", " + gene.getName());

        // Create gene node and save gene UID
        Node geneNode = NodeBuilder.newNode(uid, gene);

        // Model transcripts
        if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
            pwRel = csv.getCsvWriters().get(Relation.Type.HAS___GENE___TRANSCRIPT.toString());
            PrintWriter pwTranscr = csv.getCsvWriters().get(Node.Type.TRANSCRIPT.toString());
            for (Transcript transcript: gene.getTranscripts()) {
                Long transcrUid = csv.getLong(transcript.getId(), Node.Type.TRANSCRIPT.name());
                if (transcrUid == null) {
                    // Create gene node and write the CSV file
                    Node transcrNode = createTranscriptNode(transcript);
                    transcrUid = transcrNode.getUid();
                    pwTranscr.println(csv.nodeLine(transcrNode));
                }

                // Write gene-transcript relation
                pwRel.println(uid + CsvInfo.SEPARATOR + transcrUid);
            }
        }

        // Model miRNA gene and mature miRNA
        if (gene.getMirna() != null) {
            MiRnaGene miRna = gene.getMirna();
            PrintWriter pwRelGeneMiRna = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.IS___GENE___MIRNA.toString());
            PrintWriter pwMiRna = csv.getCsvWriters().get(Node.Type.MIRNA.toString());
            Long miRnaUid = csv.getLong(miRna.getId(), Node.Type.MIRNA.name());
            if (miRnaUid == null) {
                // Create miRNA node and write the CSV file
                miRnaUid = csv.getAndIncUid();
                Node miRnaNode = NodeBuilder.newNode(miRnaUid, miRna);

                csv.putLong(miRna.getId(), Node.Type.MIRNA.name(), miRnaUid);
                pwMiRna.println(csv.nodeLine(miRnaNode));

                // Mature miRna
                if (CollectionUtils.isNotEmpty(miRna.getMatures())) {
                    PrintWriter pwMature = csv.getCsvWriters().get(Node.Type.MIRNA_MATURE.toString());
                    PrintWriter pwRelMatureMiRna = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.MATURE___MIRNA___MIRNA_MATURE.toString());
                    for (MiRnaMature mature : miRna.getMatures()) {
                        Long matureUid = csv.getLong(mature.getId(), Node.Type.MIRNA_MATURE.name());
                        if (matureUid == null) {
                            matureUid = csv.getAndIncUid();
                            // Create mature miRNA node and write the CSV file
                            Node matureNode = NodeBuilder.newNode(matureUid, mature);
                            csv.putLong(mature.getId(), Node.Type.MIRNA_MATURE.name(), matureUid);
                            pwMature.println(csv.nodeLine(matureNode));
                        }
                        // Write mirna-mature relation
                        pwRelMatureMiRna.println(miRnaUid + CsvInfo.SEPARATOR + matureUid);
                    }
                }
            }
            // Write gene-mirna relation
            pwRelGeneMiRna.println(uid + CsvInfo.SEPARATOR + miRnaUid);
        }

        // Model gene annotation: drugs, diseases,...
        if (gene.getAnnotation() != null) {
            // Model drugs
            if (CollectionUtils.isNotEmpty(gene.getAnnotation().getDrugs())) {
                pwRel = csv.getCsvWriters().get(Relation.Type.ANNOTATION___DRUG___GENE_DRUG_INTERACTION.toString());
                for (GeneDrugInteraction drugInteraction : gene.getAnnotation().getDrugs()) {
                    // Gene drug interaction node
                    n = NodeBuilder.newNode(csv.getAndIncUid(), drugInteraction);
                    updateCSVFiles(uid, n, Relation.Type.ANNOTATION___GENE___GENE_DRUG_INTERACTION.toString());

                    // Drug node
                    Long drugUid = csv.getLong(drugInteraction.getDrugName(), Node.Type.DRUG.name());
                    if (drugUid == null) {
                        Node drugNode = new Node(csv.getAndIncUid(), drugInteraction.getDrugName(), drugInteraction.getChemblId(),
                                Node.Type.DRUG);
                        csv.getCsvWriters().get(Node.Type.DRUG.toString()).println(csv.nodeLine(drugNode));

                        // Save drug and gene-drug UIDs
                        drugUid = drugNode.getUid();
                        csv.putLong(drugNode.getName(), Node.Type.DRUG.name(), drugUid);
                        csv.putLong(drugNode.getId(), Node.Type.DRUG.name(), drugUid);
                    }
                    pwRel.println(csv.relationLine(drugUid, n.getUid()));
                }
            }

            // Model gene trait association (diseases)
            if (CollectionUtils.isNotEmpty(gene.getAnnotation().getDiseases())) {
                pwRel = csv.getCsvWriters().get(Relation.Type.ANNOTATION___GENE___GENE_TRAIT_ASSOCIATION.toString());
                Set<String> done = new HashSet<>();
                for (GeneTraitAssociation disease : gene.getAnnotation().getDiseases()) {
                    String diseaseId = disease.getId() + (disease.getHpo() != null ? "_" + disease.getHpo() : "");
                    // It is possible that the same trait association is several times in the list
                    if (!done.contains(diseaseId)) {
                        Long diseaseUid = csv.getLong(diseaseId, Node.Type.GENE_TRAIT_ASSOCIATION.name());
                        if (diseaseUid == null) {
                            n = NodeBuilder.newNode(csv.getAndIncUid(), disease);
                            updateCSVFiles(uid, n, Relation.Type.ANNOTATION___GENE___GENE_TRAIT_ASSOCIATION.toString());

                            csv.putLong(diseaseId, Node.Type.GENE_TRAIT_ASSOCIATION.name(), n.getUid());
                        } else {
                            // create gene-disease relation
                            pwRel.println(csv.relationLine(uid, diseaseUid));
                        }
                        done.add(diseaseId);
                    }
                }
            }

//            // Model constraint
//            if (CollectionUtils.isNotEmpty(gene.getAnnotation().getConstraints())) {
//                for (Constraint constraint : gene.getAnnotation().getConstraints()) {
//                    n = NodeBuilder.newNode(csv.getAndIncUid(), constraint);
//                    // Write constraint node and gene-constraint relation
//                    updateCSVFiles(uid, n, Relation.Type.GENE__CONSTRAINT.toString());
//                }
//            }

            // Gene expression
            if (CollectionUtils.isNotEmpty(gene.getAnnotation().getExpression())) {
                for (Expression expression : gene.getAnnotation().getExpression()) {
                    n = NodeBuilder.newNode(csv.getAndIncUid(), expression);
                    // Write gene and expression relation
                    updateCSVFiles(uid, n, Relation.Type.ANNOTATION___GENE___GENE_EXPRESSION.toString());
                }
            }
        }

        // Xrefs
        PrintWriter pwXref = csv.getCsvWriters().get(Node.Type.XREF.toString());
        pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.ANNOTATION___GENE___XREF.toString());
        Set<Xref> xrefSet = new HashSet<>();
        xrefSet.add(new Xref(gene.getId(), "Ensembl", "Ensembl"));
        xrefSet.add(new Xref(gene.getName(), "Ensembl", "Ensembl"));
        if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
            for (Transcript transcript : gene.getTranscripts()) {
                if (CollectionUtils.isNotEmpty(transcript.getXrefs())) {
                    for (Xref xref : transcript.getXrefs()) {
                        xrefSet.add(xref);
                    }
                }
            }
        }
        Iterator<Xref> it = xrefSet.iterator();
        while (it.hasNext()) {
            Xref xref = it.next();
            Long xrefUid = csv.getLong(xref.getDbName() + "." + xref.getId(), Node.Type.XREF.name());
            if (xrefUid == null) {
                n = NodeBuilder.newNode(csv.getAndIncUid(), xref);
                pwXref.println(csv.nodeLine(n));
                xrefUid = n.getUid();
                csv.putLong(xref.getDbName() + "." + xref.getId(), Node.Type.XREF.name(), xrefUid);
            }
            pwRel.println(csv.relationLine(uid, xrefUid));
        }

        return geneNode;
    }

    private Node completeProteinNode(Node node) {
        Entry protein = csv.getProtein(node.getName());
        if (protein == null) {
            return node;
        }

        // Create protein node and add the attributes from the previous node
        Node newNode = createProteinNode(protein, node.getUid());
        for (String key: node.getAttributes().keySet()) {
            newNode.addAttribute(key, node.getAttributes().get(key));
        }

        String geneId = csv.getGeneCache().getPrimaryId(node.getName());
        if (StringUtils.isNotEmpty(geneId)) {
            processGene(geneId, geneId);
        }

        return newNode;
    }

    private Node createProteinNode(Entry protein) {
        return createProteinNode(protein, csv.getAndIncUid());
    }

    private Node createProteinNode(Entry protein, Long uid) {
        Node n;
        PrintWriter pw;

        // Create protein node and save protein UID
        Node proteinNode = NodeBuilder.newNode(uid, protein);

        // Model protein keywords
        if (CollectionUtils.isNotEmpty(protein.getKeyword())) {
            pw = csv.getCsvWriters().get(Relation.Type.ANNOTATION___PROTEIN___PROTEIN_KEYWORD.toString());
            for (KeywordType keyword: protein.getKeyword()) {
                Long kwUid = csv.getLong(keyword.getId(), Node.Type.PROTEIN_KEYWORD.name());
                if (kwUid == null) {
                    n = new Node(csv.getAndIncUid(), keyword.getId(), keyword.getValue(), Node.Type.PROTEIN_KEYWORD);
                    updateCSVFiles(uid, n, Relation.Type.ANNOTATION___PROTEIN___PROTEIN_KEYWORD.toString());
                    csv.putLong(keyword.getId(), Node.Type.PROTEIN_KEYWORD.name(), n.getUid());
                } else {
                    // Create protein - protein keyword relation
                    pw.println(csv.relationLine(uid, kwUid));
                }
            }
        }

        // Model protein features
        if (CollectionUtils.isNotEmpty(protein.getFeature())) {
            pw = csv.getCsvWriters().get(Relation.Type.ANNOTATION___PROTEIN___PROTEIN_FEATURE.toString());
            for (FeatureType feature: protein.getFeature()) {
                if (StringUtils.isNotEmpty(feature.getId())) {
                    Long featureUid = csv.getLong(feature.getId(), Node.Type.PROTEIN_FEATURE.name());
                    if (featureUid == null) {
                        n = NodeBuilder.newNode(csv.getAndIncUid(), feature);
                        updateCSVFiles(uid, n, Relation.Type.ANNOTATION___PROTEIN___PROTEIN_FEATURE.toString());
                        csv.putLong(feature.getId(), Node.Type.PROTEIN_FEATURE.name(), n.getUid());
                    } else {
                        // Create protein - protein feature relation
                        pw.println(csv.relationLine(uid, featureUid));
                    }
                } else {
                    n = NodeBuilder.newNode(csv.getAndIncUid(), feature);
                    updateCSVFiles(uid, n, Relation.Type.ANNOTATION___PROTEIN___PROTEIN_FEATURE.toString());
                }
            }
        }

        // Model Xrefs
        if (CollectionUtils.isNotEmpty(protein.getDbReference())) {
            PrintWriter pwXref = csv.getCsvWriters().get(Node.Type.XREF.toString());
            pw = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.ANNOTATION___PROTEIN___XREF.toString());
            for (DbReferenceType dbRef: protein.getDbReference()) {
                Long xrefUid = csv.getLong(dbRef.getType() + "." + dbRef.getId(), Node.Type.XREF.name());
                if (xrefUid == null) {
                    n = NodeBuilder.newNode(csv.getAndIncUid(), dbRef);
                    pwXref.println(csv.nodeLine(n));
                    xrefUid = n.getUid();
                    csv.putLong(dbRef.getType() + "." + dbRef.getId(), Node.Type.XREF.name(), xrefUid);
                }
                pw.println(csv.relationLine(uid, xrefUid));
            }
        }

        // Return node
        return proteinNode;
    }

    private Node createTranscriptNode(Transcript transcript) {
        return createTranscriptNode(transcript, csv.getAndIncUid());
    }

    private Node createTranscriptNode(Transcript transcript, Long uid) {
        // Create transcript node and save transcript UId
        Node node = NodeBuilder.newNode(uid, transcript);
        csv.putLong(transcript.getId(), Node.Type.TRANSCRIPT.name(), uid);

        Node n;

        // Get protein, remember that all proteins were created before genes/transcripts
        if (CollectionUtils.isNotEmpty(transcript.getXrefs())) {
            Long proteinUid = null;
            for (Xref xref: transcript.getXrefs()) {
                String proteinId = csv.getProteinCache().getPrimaryId(xref.getId());
                if (proteinId != null) {
                    proteinUid = csv.getLong(proteinId, Node.Type.PROTEIN.name());
//                    if (proteinUid == null) {
//                        Entry protein = csv.getProtein(proteinId);
//                        if (protein != null) {
//                            // Create protein node and write the CSV file
//                            Node proteinNode = createProteinNode(protein);
//                            csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).println(csv.nodeLine(proteinNode));
//                            proteinUid = proteinNode.getUid();
//
//                            // Save protein UID
//                            csv.saveProteinUid(proteinId, proteinUid);
//                        } else {
//                            logger.info("Protein not found for ID {}", proteinId);
//                        }
//                    }

                    if (proteinUid != null) {
                        // Write transcript-protein relation
                        csv.getCsvWriters().get(Relation.Type.IS___TRANSCRIPT___PROTEIN.toString()).println(uid + CsvInfo.SEPARATOR
                                + proteinUid);
                        break;
                    }
                }
            }
//            if (proteinUid == null && StringUtils.isNotEmpty(transcript.getProteinId())) {
//                System.out.println("Protein not found!!! Transcript " + transcript.getId() + " with proteinId = "
//                        + transcript.getProteinId());
//            }
        }

        // Model exon
        if (CollectionUtils.isNotEmpty(transcript.getExons())) {
            PrintWriter pwRel = csv.getCsvWriters().get(Relation.Type.HAS___TRANSCRIPT___EXON.toString());
            for (Exon exon : transcript.getExons()) {
                Long exonUid = csv.getLong(exon.getId(), Node.Type.EXON.name());
                if (exonUid == null) {
                    n = createExonNode(exon, transcript.getSource());
                    exonUid = n.getUid();
                }
                // Add relation transcript-ontology to CSV file
                pwRel.println(uid + CsvInfo.SEPARATOR + exonUid);
            }
        }

        // TFBS
        if (CollectionUtils.isNotEmpty(transcript.getTfbs())) {
            for (TranscriptTfbs tfbs: transcript.getTfbs()) {
                n = NodeBuilder.newNode(csv.getAndIncUid(), tfbs);
                updateCSVFiles(uid, n, Relation.Type.ANNOTATION___TRANSCRIPT___TFBS.toString());
            }
        }

        if (transcript.getAnnotation() != null) {
            // Model constraint
            if (CollectionUtils.isNotEmpty(transcript.getAnnotation().getConstraints())) {
                Set<String> done = new HashSet<>();
                for (Constraint constraint : transcript.getAnnotation().getConstraints()) {
                    String constraintId = constraint.getName() + "." + constraint.getSource() + "." + constraint.getValue();
                    if (!done.contains(constraintId)) {
                        Long constraintUid = checkConstraint(constraint);
                        csv.getCsvWriters().get(Relation.Type.ANNOTATION___TRANSCRIPT___TRANSCRIPT_CONSTRAINT_SCORE.name()).println(
                                csv.relationLine(uid, constraintUid));
                        done.add(constraintId);
                    }
                }
            }
//
//
//
//                    n = NodeBuilder.newNode(csv.getAndIncUid(), constraint);
//                    // Write constraint node and transcript-constraint relation
//                    updateCSVFiles(uid, n, Relation.Type.ANNOTATION___TRANSCRIPT___TRANSCRIPT_CONSTRAINT_SCORE.toString());
//                }
//        }

            // Model feature ontology term annotation
            if (CollectionUtils.isNotEmpty(transcript.getAnnotation().getOntologies())) {
                PrintWriter pwRel = csv.getCsvWriters().get(Relation.Type.ANNOTATION___TRANSCRIPT___FEATURE_ONTOLOGY_TERM_ANNOTATION
                        .toString());
                for (FeatureOntologyTermAnnotation ontology : transcript.getAnnotation().getOntologies()) {
                    // Create the feature ontology term annotation node and the relation with the transcript to CSV file
                    n = createFeatureOntologyTermAnnotationNode(ontology);
                    pwRel.println(uid + CsvInfo.SEPARATOR + n.getUid());

//                    Long ontologyUid = csv.getLong(ontology.getId(), Node.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION.name());
//                    if (ontologyUid == null) {
//                        n = createFeatureOntologyTermAnnotationNode(ontology);
//                        ontologyUid = n.getUid();
//                    }
//                    // Add relation transcript-ontology to CSV file
//                    pwRel.println(uid + CsvInfo.SEPARATOR + ontologyUid);
                }
            }
        }

        // Model Xrefs
        if (CollectionUtils.isNotEmpty(transcript.getXrefs())) {
            PrintWriter pwXref = csv.getCsvWriters().get(Node.Type.XREF.toString());
            PrintWriter pw = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.ANNOTATION___TRANSCRIPT___XREF.toString());
            Set<String> done = new HashSet<>();
            for (Xref xref: transcript.getXrefs()) {
                String xrefId = xref.getDbName() + "." + xref.getId();
                // In the list, one xref can be multiple times :(
                if (!done.contains(xrefId)) {
                    Long xrefUid = csv.getLong(xrefId, Node.Type.XREF.name());
                    if (xrefUid == null) {
                        n = NodeBuilder.newNode(csv.getAndIncUid(), xref);
                        pwXref.println(csv.nodeLine(n));
                        xrefUid = n.getUid();
                        csv.putLong(xrefId, Node.Type.XREF.name(), xrefUid);
                    }
                    pw.println(csv.relationLine(uid, xrefUid));
                    done.add(xrefId);
                }
            }
        }


        return node;
    }

    private Node createExonNode(Exon exon, String source) {
        return createExonNode(exon, csv.getAndIncUid(), source);
    }

    private Node createExonNode(Exon exon, Long uid, String source) {
        // Create exon node and save UId and wrrite CSV file
        Node node = NodeBuilder.newNode(uid, exon, source);
        csv.getCsvWriters().get(Node.Type.EXON.toString()).println(csv.nodeLine(node));
        csv.putLong(exon.getId(), Node.Type.EXON.name(), uid);

        return node;
    }

    private Node createFeatureOntologyTermAnnotationNode(FeatureOntologyTermAnnotation ontology) {
        return createFeatureOntologyTermAnnotationNode(ontology, csv.getAndIncUid());
    }

    private Node createFeatureOntologyTermAnnotationNode(FeatureOntologyTermAnnotation ontology, Long uid) {
        // Create ontology node and save UId and wrrite CSV file
        Node node = NodeBuilder.newNode(uid, ontology);
        csv.getCsvWriters().get(Node.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION.toString()).println(csv.nodeLine(node));
        csv.putLong(ontology.getId(), Node.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION.name(), uid);

        if (CollectionUtils.isNotEmpty(ontology.getEvidence())) {
            for (AnnotationEvidence annotationEvidence : ontology.getEvidence()) {
                Node n = NodeBuilder.newNode(csv.getAndIncUid(), annotationEvidence);
                // Write evidence node and ontology-evidence relation
                updateCSVFiles(uid, n, Relation.Type.HAS___FEATURE_ONTOLOGY_TERM_ANNOTATION___TRANSCRIPT_ANNOTATION_EVIDENCE.toString());
            }
        }

        return node;
    }

    private Long processVariant(Variant variant) {
        Node variantNode = null;

        Long variantUid = csv.getLong(variant.toStringSimple(), Node.Type.VARIANT.name());
        if (variantUid == null) {
            variantNode = createVariantNode(variant);
            variantUid = variantNode.getUid();
            csv.putLong(variant.toStringSimple(), Node.Type.VARIANT.name(), variantUid);
        }

        // Process sample info
//        processSampleInfo(variant, variantUid);

        return variantUid;
    }

    private Node createVariantNode(Variant variant) {
        return createVariantNode(variant, csv.getAndIncUid());
    }

    private Node createVariantNode(Variant variant, Long varUid) {
        Node varNode = NodeBuilder.newNode(varUid, variant);
        PrintWriter pw = csv.getCsvWriters().get(Node.Type.VARIANT.toString());
        pw.println(csv.nodeLine(varNode));

        Node node;

        // Create study related nodes
        createStudyRelatedNodes(varUid, variant);

        //        // Processing clinical variants
//        logger.info("Processing clinical variants...");
//        start = System.currentTimeMillis();
//        buildVariants(clinicalVariantFile.toPath());
//        logger.info("Processing clinical variants done in {} s", (System.currentTimeMillis() - start) / 1000);

        // Structural variant
        if (variant.getSv() != null) {
            node = NodeBuilder.newNode(csv.getAndIncUid(), variant.getSv(), csv);
            updateCSVFiles(varUid, node, Relation.Type.HAS___VARIANT___STRUCTURAL_VARIANT.toString());
        }

        // Annotation management
        if (variant.getAnnotation() != null) {
            VariantAnnotation annotation = variant.getAnnotation();
            // Consequence types
            if (CollectionUtils.isNotEmpty(annotation.getConsequenceTypes())) {
                // Consequence type nodes
                for (ConsequenceType ct : annotation.getConsequenceTypes()) {
                    Node ctNode = NodeBuilder.newNode(csv.getAndIncUid(), ct);
                    updateCSVFiles(varUid, ctNode, Relation.Type.ANNOTATION___VARIANT___VARIANT_CONSEQUENCE_TYPE.toString());

                    // Gene
                    Long geneUid = processGene(ct.getEnsemblGeneId(), ct.getGeneName());
                    if (geneUid != null) {
                        // Relation: consequence type - gene
                        pw = csv.getCsvWriters().get(Relation.Type.ANNOTATION___VARIANT_CONSEQUENCE_TYPE___GENE.toString());
                        pw.println(csv.relationLine(ctNode.getUid(), geneUid));
                    }

                    // Transcript
                    Long transcriptUid = processTranscript(ct.getEnsemblTranscriptId());
                    if (transcriptUid != null) {
                        // Relation: consequence type - transcript
                        pw = csv.getCsvWriters().get(Relation.Type.ANNOTATION___VARIANT_CONSEQUENCE_TYPE___TRANSCRIPT.toString());
                        pw.println(csv.relationLine(ctNode.getUid(), transcriptUid));
                    } else {
                        if (geneUid != null) {
                            logger.info("Transcript UID is null for {} (gene {}), maybe version mismatch!!"
                                            + " Gene vs variant annotation!!!", ct.getEnsemblTranscriptId(),
                                    ct.getEnsemblGeneId());
                        }
                    }

                    // SO_TERM
                    if (CollectionUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                            String soId = so.getAccession();
                            if (soId != null) {
                                Long soUid = csv.getLong(soId, Node.Type.SO_TERM.name());
                                if (soUid == null) {
                                    Node soNode = new Node(csv.getAndIncUid(), so.getAccession(), so.getName(), Node.Type.SO_TERM);
                                    updateCSVFiles(ctNode.getUid(), soNode, Relation.Type.ANNOTATION___VARIANT_CONSEQUENCE_TYPE___SO_TERM
                                            .toString());
                                    csv.putLong(soId, Node.Type.SO_TERM.name(), soNode.getUid());
                                } else {
                                    // Relation: consequence type - so
                                    pw = csv.getCsvWriters().get(Relation.Type.ANNOTATION___VARIANT_CONSEQUENCE_TYPE___SO_TERM.toString());
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
                                Relation.Type.ANNOTATION___VARIANT_CONSEQUENCE_TYPE___PROTEIN_VARIANT_ANNOTATION.toString());

                        // Protein relationship management
                        Long protUid = processProtein(ct.getProteinVariantAnnotation().getUniprotAccession(),
                                ct.getProteinVariantAnnotation().getUniprotName());
                        if (protUid != null) {
                            // Relation: protein variant annotation - protein
                            pw = csv.getCsvWriters().get(Relation.Type.ANNOTATION___PROTEIN_VARIANT_ANNOTATION___PROTEIN.toString());
                            pw.println(csv.relationLine(pVANode.getUid(), protUid));
                        }

                        // Protein substitution scores
                        if (CollectionUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                            for (Score score : ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                Node scoreNode = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.PROTEIN_SUBSTITUTION_SCORE);
                                updateCSVFiles(pVANode.getUid(), scoreNode,
                                        Relation.Type.ANNOTATION___PROTEIN_VARIANT_ANNOTATION___PROTEIN_SUBSTITUTION_SCORE.toString());
                            }
                        }
                    }
                }
            }

            // HGVS
            if (CollectionUtils.isNotEmpty(annotation.getHgvs())) {
                for (String hgv : annotation.getHgvs()) {
                    // HGV node
                    node = new Node(csv.getAndIncUid(), hgv, hgv, Node.Type.HGV);
                    updateCSVFiles(varUid, node, Relation.Type.ANNOTATION___VARIANT___HGV.toString());
                }
            }

            // Variant population frequencies
            if (CollectionUtils.isNotEmpty(annotation.getPopulationFrequencies())) {
                for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                    // Population frequency node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), popFreq);
                    updateCSVFiles(varUid, node, Relation.Type.ANNOTATION___VARIANT___VARIANT_POPULATION_FREQUENCY.toString());
                }
            }

            // Variant conservation scores
            if (CollectionUtils.isNotEmpty(annotation.getConservation())) {
                for (Score score : annotation.getConservation()) {
                    // Conservation node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.VARIANT_CONSERVATION_SCORE);
                    updateCSVFiles(varUid, node, Relation.Type.ANNOTATION___VARIANT___VARIANT_CONSERVATION_SCORE.toString());
                }
            }

            // Clinical evidence
            if (CollectionUtils.isNotEmpty(annotation.getTraitAssociation())) {
                for (EvidenceEntry evidence : annotation.getTraitAssociation()) {
                    // Clinical evidence node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), evidence, Node.Type.CLINICAL_EVIDENCE, csv);
                    updateCSVFiles(varUid, node, Relation.Type.ANNOTATION___VARIANT___CLINICAL_EVIDENCE.toString());
                }
            }

            // Variant functional scores
            if (CollectionUtils.isNotEmpty(annotation.getFunctionalScore())) {
                for (Score score : annotation.getFunctionalScore()) {
                    // Functional score node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.VARIANT_FUNCTIONAL_SCORE);
                    updateCSVFiles(varUid, node, Relation.Type.ANNOTATION___VARIANT___VARIANT_FUNCTIONAL_SCORE.toString());
                }
            }

            // Repeat
            if (CollectionUtils.isNotEmpty(annotation.getRepeat())) {
                for (Repeat repeat : annotation.getRepeat()) {
                    if (StringUtils.isNotEmpty(repeat.getId())) {
                        String repeatId = repeat.getId() + "." + repeat.getChromosome() + "." + repeat.getStart() + "." + repeat.getEnd();
                        Long repeatUid = csv.getLong(repeatId, Node.Type.REPEAT.name());
                        if (repeatUid == null) {
                            node = NodeBuilder.newNode(csv.getAndIncUid(), repeat);
                            csv.getCsvWriters().get(Node.Type.REPEAT.name()).println(csv.nodeLine(node));
                            repeatUid = node.getUid();
                            csv.putLong(repeatId, Node.Type.REPEAT.name(), repeatUid);
                        }
                        csv.getCsvWriters().get(Relation.Type.ANNOTATION___VARIANT___REPEAT.name()).println(csv.relationLine(varUid,
                                repeatUid));
                    }
                }
            }

            // Cytoband
            if (CollectionUtils.isNotEmpty(annotation.getCytoband())) {
                for (Cytoband cytoband : annotation.getCytoband()) {
                    if (StringUtils.isNotEmpty(cytoband.getName())) {
                        Long cytobandUid = csv.getLong(cytoband.getName(), Node.Type.CYTOBAND.name());
                        if (cytobandUid == null) {
                            node = NodeBuilder.newNode(csv.getAndIncUid(), cytoband);
                            csv.getCsvWriters().get(Node.Type.CYTOBAND.name()).println(csv.nodeLine(node));
                            cytobandUid = node.getUid();
                            csv.putLong(cytoband.getName(), Node.Type.CYTOBAND.name(), cytobandUid);
                        }
                        csv.getCsvWriters().get(Relation.Type.ANNOTATION___VARIANT___CYTOBAND.name()).println(csv.relationLine(varUid,
                                cytobandUid));
                    }
                }
            }

            // Variant drug interaction
            if (CollectionUtils.isNotEmpty(annotation.getDrugs())) {
                for (Drug variantDrugInteraction : annotation.getDrugs()) {
                    node = NodeBuilder.newNode(csv.getAndIncUid(), variantDrugInteraction);
                    updateCSVFiles(varUid, node, Relation.Type.ANNOTATION___VARIANT___VARIANT_DRUG_INTERACTION.toString());
                }
            }

            // Constraints
            if (CollectionUtils.isNotEmpty(annotation.getGeneConstraints())) {
                Set<String> done = new HashSet<>();
                for (Constraint constraint : annotation.getGeneConstraints()) {
                    String constraintId = constraint.getName() + "." + constraint.getSource() + "." + constraint.getValue();
                    if (!done.contains(constraintId)) {
                        Long constraintUid = checkConstraint(constraint);
                        csv.getCsvWriters().get(Relation.Type.ANNOTATION___VARIANT___TRANSCRIPT_CONSTRAINT_SCORE.name()).println(
                                csv.relationLine(varUid, constraintUid));
                        done.add(constraintId);
                    }
                }
            }
        }

        return varNode;
    }

    private void createStudyRelatedNodes(Long varUid, Variant variant) {
        if (CollectionUtils.isEmpty(variant.getStudies())) {
            return;
        }

        for (StudyEntry studyEntry : variant.getStudies()) {
            // Process file data
            if (CollectionUtils.isNotEmpty(studyEntry.getFiles())) {
                for (FileEntry fileEntry : studyEntry.getFiles()) {
                    Long fileUid = csv.getLong(fileEntry.getFileId(), Node.Type.VARIANT_FILE.name());
                    if (fileUid != null) {
                        Node fileDataNode = new Node(csv.getAndIncUid(), fileEntry.getFileId(), "", Node.Type.VARIANT_FILE_DATA);
                        if (MapUtils.isNotEmpty(fileEntry.getData())) {
                            for (Map.Entry<String, String> entry : fileEntry.getData().entrySet()) {
                                fileDataNode.addAttribute(entry.getKey(), entry.getValue());
                            }
                        }
                        // Write variant file data node and the relation with variant
                        updateCSVFiles(varUid, fileDataNode, Relation.Type.DATA___VARIANT___VARIANT_FILE_DATA.toString());

                        // and the relation with file
                        csv.getCsvWriters().get(Relation.Type.DATA___VARIANT_FILE___VARIANT_FILE_DATA.toString()).println(csv.relationLine(
                                fileUid, fileDataNode.getUid()));
                    } else {
                        logger.info("File ID " + fileEntry.getFileId() + " for variant ID " + variant.toString() + ". Can't make relation: "
                                + " File - Variant File Data");
                    }
                }
            }

            // Process sample data
            if (CollectionUtils.isNotEmpty(studyEntry.getSamples())) {
                for (int sampleIndex = 0; sampleIndex < studyEntry.getSamples().size(); sampleIndex++) {
                    SampleEntry sampleEntry = studyEntry.getSamples().get(sampleIndex);
                    String sampleId = sampleIds.get(sampleIndex);
                    Long sampleUid = csv.getLong(sampleId, Node.Type.SAMPLE.name());
                    if (sampleUid != null) {
                        Node sampleDataNode = new Node(csv.getAndIncUid(), sampleId, "", Node.Type.VARIANT_SAMPLE_DATA);
                        if (CollectionUtils.isNotEmpty(sampleEntry.getData())) {
                            for (int i = 0; i < sampleEntry.getData().size(); i++) {
                                sampleDataNode.addAttribute(studyEntry.getSampleDataKeys().get(i), sampleEntry.getData().get(i));
                            }
                        }
                        // Write variant sample data node and the relation with variant
                        updateCSVFiles(varUid, sampleDataNode, Relation.Type.DATA___VARIANT___VARIANT_SAMPLE_DATA.toString());

                        // and the relation with sample
                        csv.getCsvWriters().get(Relation.Type.DATA___SAMPLE___VARIANT_SAMPLE_DATA.toString()).println(csv.relationLine(
                                sampleUid, sampleDataNode.getUid()));

                        // and add relation variant file - sample
                        if (sampleEntry.getFileIndex() != null && CollectionUtils.isNotEmpty(studyEntry.getFiles())) {
                            if (sampleEntry.getFileIndex() < studyEntry.getFiles().size()) {
                                String fileId = studyEntry.getFiles().get(sampleEntry.getFileIndex()).getFileId();
                                Long fileUid = csv.getLong(fileId, Node.Type.VARIANT_FILE.name());
                                if (fileUid != null) {
                                    // First, check that this relation does not exist yet
                                    Long relUid = csv.getLong(fileId + "." + sampleId, Relation.Type.HAS___VARIANT_FILE___SAMPLE.name());
                                    if (relUid == null) {
                                        // and the relation with sample
                                        csv.getCsvWriters().get(Relation.Type.HAS___VARIANT_FILE___SAMPLE.toString()).println(
                                                csv.relationLine(fileUid, sampleUid));

                                        // Save this relation
                                        csv.putLong(fileId + "." + sampleId, Relation.Type.HAS___VARIANT_FILE___SAMPLE.name(), 1);
                                    }
                                }
                            }
                        }

                    } else {
                        logger.info("Sample ID " + sampleEntry.getSampleId() + " for variant ID " + variant.toString() + ". Can't make "
                                + "relation: Sample - Variant Sample Data");
                    }
                }
            }
        }
    }


    private void processAdditionalNetwork(String additionalNeworkFilename) throws IOException {
        // Check file
        File addNetworkFile = Paths.get(additionalNeworkFilename).toFile();
        if (!addNetworkFile.exists()) {
            logger.info("Additional network file {} does not exist", additionalNeworkFilename);
            return;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Network network = objectMapper.readValue(addNetworkFile, Network.class);

        Map<Long, Long> nodeUidMap = new HashMap<>();

        // First, nodes
        if (CollectionUtils.isNotEmpty(network.getNodes())) {
            for (Node node: network.getNodes()) {
                Long uid = csv.getLong(node.getId(), node.getType().name());
                if (uid == null) {
                    // Node does not exist in the !
                    nodeUidMap.put(node.getUid(), csv.getAndIncUid());
                    // Update UID and append node to the CSV file
                    node.setUid(nodeUidMap.get(node.getUid()));
                    csv.getCsvWriters().get(node.getType().toString()).println(csv.nodeLine(node));
                } else {
                    // Node already exists !!
                    nodeUidMap.put(node.getUid(), uid);
                }
            }
        }

        // Second, relations
        if (CollectionUtils.isNotEmpty(network.getRelations())) {
            for (Relation relation: network.getRelations()) {
                relation.setUid(csv.getAndIncUid());
                csv.getCsvWriters().get(relation.getType().toString()).println(csv.relationLine(nodeUidMap.get(relation.getOrigUid()),
                        nodeUidMap.get(relation.getDestUid())));
            }
        }
    }

    private Long checkConstraint(Constraint constraint) {
        Long constraintUid = csv.getLong(constraint.getName() + "." + constraint.getValue(), Node.Type.TRANSCRIPT_CONSTRAINT_SCORE.name());
        if (constraintUid == null) {
            Node node = NodeBuilder.newNode(csv.getAndIncUid(), constraint);
            csv.getCsvWriters().get(Node.Type.TRANSCRIPT_CONSTRAINT_SCORE.name()).println(csv.nodeLine(node));
            constraintUid = node.getUid();
            csv.putLong(constraint.getName() + "." + constraint.getValue(), Node.Type.TRANSCRIPT_CONSTRAINT_SCORE.name(),
                    constraintUid);
        }
        return constraintUid;
    }


//
//    public Long processClinicalAnalysis(ClinicalAnalysis clinicalAnalysis) throws IOException {
//        Node clinicalAnalysisNode = null;
//        Long clinicalAnalysisUid = csv.getLong(clinicalAnalysis.getId());
//        if (clinicalAnalysisUid == null) {
//            clinicalAnalysisNode = createClinicalAnalysisNode(clinicalAnalysis);
//            clinicalAnalysisUid = clinicalAnalysisNode.getUid();
//            csv.putLong(clinicalAnalysis.getId(), clinicalAnalysisUid);
//        }
//
//        return clinicalAnalysisUid;
//    }
//
//    public Node createClinicalAnalysisNode(ClinicalAnalysis clinicalAnalysis) throws IOException {
//        return createClinicalAnalysisNode(clinicalAnalysis, csv.getAndIncUid());
//    }
//
//    public Node createClinicalAnalysisNode(ClinicalAnalysis clinicalAnalysis, Long caUid) throws IOException {
//        Node caNode = NodeBuilder.newNode(caUid, clinicalAnalysis);
//        PrintWriter pw = csv.getCsvWriters().get(Node.Type.CLINICAL_ANALYSIS.toString());
//        pw.println(csv.nodeLine(caNode));
//
//        // Comments
//        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getComments())) {
//            for (Comment comment : clinicalAnalysis.getComments()) {
//                Node commentNode = NodeBuilder.newNode(csv.getAndIncUid(), comment);
//                pw = csv.getCsvWriters().get(Node.Type.COMMENT.toString());
//                pw.println(csv.nodeLine(commentNode));
//
//                // Relation: clinical analysis - comment
//                pw = csv.getCsvWriters().get(Relation.Type.CLINICAL_ANALYSIS__COMMENT.toString());
//                pw.println(csv.relationLine(caUid, commentNode.getUid()));
//            }
//        }

//        // Clinical Analyst
//        if (clinicalAnalysis.getAnalyst() != null) {
//            Node analystNode = NodeBuilder.newNode(csv.getAndIncUid(), clinicalAnalysis.getAnalyst());
//            pw = csv.getCsvWriters().get(Node.Type.CLINICAL_ANALYST.toString());
//            pw.println(csv.nodeLine(analystNode));
//
//            // Relation: clinical analysis - comment
//            pw = csv.getCsvWriters().get(Relation.Type.CLINICAL_ANALYSIS__CLINICAL_ANALYST.toString());
//            pw.println(csv.relationLine(caUid, analystNode.getUid()));
//        }

//        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getInterpretations())) {
//            for (Interpretation interpretation : clinicalAnalysis.getInterpretations()) {
//                Node interpretationNode = NodeBuilder.newNode(csv.getAndIncUid(), interpretation);
//                pw = csv.getCsvWriters().get(Node.Type.INTERPRETATION.toString());
//                pw.println(csv.nodeLine(interpretationNode));
//
//                // Relation: clinical analysis - interpretation
//                pw = csv.getCsvWriters().get(Relation.Type.CLINICAL_ANALYSIS__INTERPRETATION.toString());
//                pw.println(csv.relationLine(caUid, interpretationNode.getUid()));
//
//                // Primary findings
//                if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
//                    processReportedVariants(interpretation.getPrimaryFindings(), interpretationNode.getUid(), true);
//                }
//
//                // Secondary findings
//                if (CollectionUtils.isNotEmpty(interpretation.getSecondaryFindings())) {
//                    processReportedVariants(interpretation.getSecondaryFindings(), interpretationNode.getUid(), false);
//                }
//
////                // Low coverage
////                if (CollectionUtils.isNotEmpty(interpretation.getLowCoverageRegions())) {
////                }
////
////                // Software
////                if (interpretation.getSoftware() != null) {
////                }
//
//                // Comments
//                if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
//                    for (Comment comment : interpretation.getComments()) {
//                        Node commentNode = NodeBuilder.newNode(csv.getAndIncUid(), comment);
//                        pw = csv.getCsvWriters().get(Node.Type.COMMENT.toString());
//                        pw.println(csv.nodeLine(commentNode));
//
//                        // Relation: clinical analysis - comment
//                        pw = csv.getCsvWriters().get(Relation.Type.INTERPRETATION__COMMENT.toString());
//                        pw.println(csv.relationLine(interpretation.getUid(), commentNode.getUid()));
//                    }
//                }
//            }
//        }
//
//
//        return caNode;
//    }

//    private void processReportedVariants(List<ClinicalVariant> findings, Long interpretationUid, boolean arePrimaryFindings)
//            throws IOException {
//        PrintWriter pw;
//        Relation.Type interpretationRelation;
//        if (arePrimaryFindings) {
//            interpretationRelation = Relation.Type.PRIMARY_FINDING___INTERPRETATION___REPORTED_VARIANT;
//
//        } else {
//            interpretationRelation = Relation.Type.SECONDARY_FINDING___INTERPRETATION___REPORTED_VARIANT;
//        }
//
//        for (ClinicalVariant finding : findings) {
//            Node findingNode = NodeBuilder.newNode(csv.getAndIncUid(), finding);
//            pw = csv.getCsvWriters().get(Node.Type.REPORTED_VARIANT.toString());
//            pw.println(csv.nodeLine(findingNode));
//
//            // Process variant and relation it to the reported variant
//            Long variantUid = processVariant(finding);
//            pw = csv.getCsvWriters().get(Relation.Type.REPORTED_VARIANT__VARIANT.toString());
//            pw.println(csv.relationLine(findingNode.getUid(), variantUid));
//
//            // Relation: interpretation - primary finding
//            pw = csv.getCsvWriters().get(interpretationRelation.toString());
//            pw.println(csv.relationLine(interpretationUid, findingNode.getUid()));
//        }
//    }


    private void buildVariants(Path path) throws IOException {
        // Reading file line by line, each line a JSON object
        BufferedReader reader = FileUtils.newBufferedReader(path);

        long counter = 0;
        ObjectMapper mapper = new ObjectMapper();
//        logger.info("Processing JSON file {}", file.getPath());
//        reader = FileUtils.newBufferedReader(file.toPath());
        String line = reader.readLine();
        while (line != null) {
            Variant variant = mapper.readValue(line, Variant.class);
            processVariant(variant);
            if (variant.getStrand().equals("-")) {
                System.out.println(variant.toStringSimple() + ", " + variant.getStrand());
            }

            // read next line
            line = reader.readLine();
            if (++counter % 5000 == 0) {
                logger.info("Parsing {} variants...", counter);
            }
        }
        reader.close();
        logger.info("Parsed {} variants from {}. Done!!!", counter, path);
    }


    //-------------------------------------------------------------------------


    private void processMetadata(List<File> variantFiles) {
        // For variant file info, variant sample format and sample nodes we have to read variant metadata files to know which attributes
        // are present
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

                    // Create individual and family nodes and relations
                    for (Individual individual : variantStudyMetadata.getIndividuals()) {
                        Long individualUid = csv.getLong(individual.getId(), Node.Type.INDIVIDUAL.name());
                        if (individualUid == null) {
                            // First, check family
                            Long familyUid = null;
                            if (StringUtils.isNotEmpty(individual.getFamily())) {
                                familyUid = csv.getLong(individual.getFamily(), Node.Type.FAMILY.name());
                                if (familyUid == null) {
                                    Node familyNode = new Node(csv.getAndIncUid(), individual.getFamily(), individual.getFamily(),
                                            Node.Type.FAMILY);
                                    csv.getCsvWriters().get(Node.Type.FAMILY.name()).println(csv.nodeLine(familyNode));
                                    familyUid = familyNode.getUid();
                                    csv.putLong(individual.getFamily(), Node.Type.FAMILY.name(), familyUid);
                                }
                            }

                            // Create individual node
                            Node individualNode = NodeBuilder.newNode(csv.getAndIncUid(), individual);
                            csv.getCsvWriters().get(Node.Type.INDIVIDUAL.name()).println(csv.nodeLine(individualNode));
                            if (familyUid != null) {
                                csv.getCsvWriters().get(Relation.Type.FAMILY_MEMBER___FAMILY___INDIVIDUAL.name()).println(
                                        csv.relationLine(individualUid, familyUid));
                            }
                            individualUid = individualNode.getUid();

                            // Create sample nodes
                            if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                                for (Sample sample : individual.getSamples()) {
                                    if (StringUtils.isNotEmpty(sample.getId())) {
                                        Node sampleNode = new Node(csv.getAndIncUid(), sample.getId(), sample.getId(), Node.Type.SAMPLE);
                                        if (MapUtils.isNotEmpty(sample.getAnnotations())) {
                                            for (Map.Entry<String, String> entry : sample.getAnnotations().entrySet()) {
                                                sampleNode.addAttribute(entry.getKey(), entry.getValue());
                                            }
                                        }
                                        csv.getCsvWriters().get(Node.Type.SAMPLE.name()).println(csv.nodeLine(sampleNode));
                                        csv.getCsvWriters().get(Relation.Type.HAS___INDIVIDUAL___SAMPLE.name()).println(
                                                csv.relationLine(individualUid, sampleNode.getUid()));

                                        // Save sample UID
                                        csv.putLong(sample.getId(), Node.Type.SAMPLE.name(), sampleNode.getUid());
                                    }
                                }
                            }
                            csv.putLong(individual.getId(), Node.Type.INDIVIDUAL.name(), individualUid);
                        }
                    }

                    // Second loop, to fulfill mother-father relations
                    for (Individual individual : variantStudyMetadata.getIndividuals()) {
                        Long individualUid = csv.getLong(individual.getId(), Node.Type.INDIVIDUAL.name());
                        if (StringUtils.isNotEmpty(individual.getMother())) {
                            Long motherUid = csv.getLong(individual.getMother(), Node.Type.INDIVIDUAL.name());
                            if (motherUid != null) {
                                csv.getCsvWriters().get(Relation.Type.MOTHER_OF___INDIVIDUAL___INDIVIDUAL.name()).println(
                                        csv.relationLine(motherUid, individualUid));
                            }
                        }

                        if (StringUtils.isNotEmpty(individual.getFather())) {
                            Long fatherUid = csv.getLong(individual.getFather(), Node.Type.INDIVIDUAL.name());
                            if (fatherUid != null) {
                                csv.getCsvWriters().get(Relation.Type.FATHER_OF___INDIVIDUAL___INDIVIDUAL.name()).println(
                                        csv.relationLine(fatherUid, individualUid));
                            }
                        }
                    }

                    // File management
                    if (CollectionUtils.isNotEmpty(variantStudyMetadata.getFiles())) {
                        for (VariantFileMetadata fileMetadata : variantStudyMetadata.getFiles()) {
                            Long fileUid = csv.getLong(fileMetadata.getId(), Node.Type.VARIANT_FILE.name());
                            if (fileUid == null) {
                                Node fileNode = new Node(csv.getAndIncUid(), fileMetadata.getId(), fileMetadata.getPath(),
                                        Node.Type.VARIANT_FILE);
                                if (MapUtils.isNotEmpty(fileMetadata.getAttributes())) {
                                    for (Map.Entry<String, String> entry : fileMetadata.getAttributes().entrySet()) {
                                        fileNode.addAttribute(entry.getKey(), entry.getValue());
                                    }
                                }
                                csv.getCsvWriters().get(Node.Type.VARIANT_FILE.name()).println(csv.nodeLine(fileNode));

                                // Save variant file UID
                                csv.putLong(fileMetadata.getId(), Node.Type.VARIANT_FILE.name(), fileNode.getUid());
                            }
                        }
                    }
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
                ? csv.getCsvAnnotatedWriters().get(node.getType().toString())
                : csv.getCsvWriters().get(node.getType().toString());
        pw.println(csv.nodeLine(node));

        // Update relation CSV file
        pw = csv.getCsvWriters().get(relationType);
        pw.println(csv.relationLine(startUid, node.getUid()));
    }

    public void buildGenes(Path genePath) throws IOException {
        GeneCache geneCache = csv.getGeneCache();
        BufferedReader reader = FileUtils.newBufferedReader(genePath);

        String jsonGene = reader.readLine();
        long geneCounter = 0;
        while (jsonGene != null) {
            Gene gene = geneCache.getObjReader().readValue(jsonGene);
            String geneId = gene.getId();
            if (StringUtils.isNotEmpty(geneId)) {
                geneCounter++;
                if (geneCounter % 5000 == 0) {
                    logger.info("Building {} genes...", geneCounter);
                }
                // Save gene
                geneCache.saveObject(geneId, jsonGene);

                // Save xrefs for that gene
                geneCache.saveXref(geneId, geneId);
                if (StringUtils.isNotEmpty(gene.getName())) {
                    geneCache.saveXref(gene.getName(), geneId);
                }

                if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                    for (Transcript transcr : gene.getTranscripts()) {
                        if (CollectionUtils.isNotEmpty(transcr.getXrefs())) {
                            for (Xref xref: transcr.getXrefs()) {
                                if (StringUtils.isNotEmpty(xref.getId())) {
                                    geneCache.saveXref(xref.getId(), geneId);
                                }
                            }
                        }
                    }
                }

                // And process gene to save to CSV file
                processGene(gene.getId(), gene.getName());
            } else {
                logger.info("Skipping indexing gene: missing gene ID from JSON file");
            }

            // Next line
            jsonGene = reader.readLine();
        }

        // Second loop
        reader.close();
        reader = FileUtils.newBufferedReader(genePath);

        jsonGene = reader.readLine();
        while (jsonGene != null) {
            Gene gene = geneCache.getObjReader().readValue(jsonGene);
            if (gene.getAnnotation() != null && CollectionUtils.isNotEmpty(gene.getAnnotation().getMirnaTargets())) {
                Long geneUid = csv.getLong(gene.getId(), Node.Type.GENE.name());
                if (geneUid != null) {
                    PrintWriter pw = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.TARGET___GENE___MIRNA_MATURE.toString());
                    for (MirnaTarget mirnaTarget : gene.getAnnotation().getMirnaTargets()) {
                        Long matureUid = csv.getLong(mirnaTarget.getSourceId(), Node.Type.MIRNA_MATURE.name());
                        if (matureUid != null) {
                            for (TargetGene target : mirnaTarget.getTargets()) {
                                Node targetNode = new Node(csv.getAndIncUid(), null, null, Node.Type.MIRNA_TARGET);
                                targetNode.addAttribute("experiment", target.getExperiment());
                                targetNode.addAttribute("evidence", target.getEvidence());
                                targetNode.addAttribute("pubmed", target.getPubmed());

                                // Write mirna target node into the CSV file
                                csv.getCsvWriters().get(Node.Type.MIRNA_TARGET.toString()).println(csv.nodeLine(targetNode));

                                // Write mirna target - gene relation
                                csv.getCsvWriters().get(Relation.Type.ANNOTATION___GENE___MIRNA_TARGET.toString()).println(geneUid
                                        + CsvInfo.SEPARATOR + targetNode.getUid());

                                // And write mirna target - mirna mature relation
                                csv.getCsvWriters().get(Relation.Type.ANNOTATION___MIRNA_MATURE___MIRNA_TARGET.toString()).println(matureUid
                                        + CsvInfo.SEPARATOR + targetNode.getUid());
                            }
                        }
                    }
                }
            }

            // Next line
            jsonGene = reader.readLine();
        }

        logger.info("Building {} genes. Done.", geneCounter);

        reader.close();
    }

    public void buildProteins(Path proteinPath) throws IOException {
        ProteinCache proteinCache = csv.getProteinCache();
        BufferedReader reader = FileUtils.newBufferedReader(proteinPath);

        String jsonProtein = reader.readLine();
        long proteinCounter = 0;
        while (jsonProtein != null) {
            Entry protein = proteinCache.getObjReader().readValue(jsonProtein);
            if (CollectionUtils.isNotEmpty(protein.getAccession())) {
                proteinCounter++;
                if (proteinCounter % 5000 == 0) {
                    logger.info("Building {} proteins...", proteinCounter);
                }

                // Save protein in RocksDB
                String proteinAcc = protein.getAccession().get(0);
                proteinCache.saveObject(proteinAcc, jsonProtein);

                // Save protein xrefs
                proteinCache.saveXref(proteinAcc, proteinAcc);
                if (CollectionUtils.isNotEmpty(protein.getAccession())) {
                    for (String acc: protein.getAccession()) {
                        proteinCache.saveXref(acc, proteinAcc);
                    }
                }

                if (protein.getProtein() != null && protein.getProtein().getRecommendedName() != null
                        && CollectionUtils.isNotEmpty(protein.getProtein().getRecommendedName().getShortName())) {
                    for (EvidencedStringType shortName : protein.getProtein().getRecommendedName().getShortName()) {
                        if (StringUtils.isNotEmpty(shortName.getValue())) {
                            proteinCache.saveXref(shortName.getValue(), proteinAcc);
                        }
                    }
                }

                String proteinName = null;
                if (CollectionUtils.isNotEmpty(protein.getName())) {
                    proteinName = protein.getName().get(0);
                    for (String name: protein.getName()) {
                        proteinCache.saveXref(name, proteinAcc);
                    }
                }

                if (CollectionUtils.isNotEmpty(protein.getDbReference())) {
                    Set<String> done = new HashSet<>();
                    for (DbReferenceType dbRef: protein.getDbReference()) {
                        // In the list, one db reference can be multiple times
                        if (!done.contains(dbRef.getId())) {
                            proteinCache.saveXref(dbRef.getId(), proteinAcc);
                            done.add(dbRef.getId());
                        }
                    }
                }

                // And process protein to save to CSV file
                processProtein(proteinAcc, proteinName);
            } else {
                logger.info("Skipping building protein: missing protein accession from JSON file");
            }

            // Next line
            jsonProtein = reader.readLine();
        }
        logger.info("Building {} proteins. Done.", proteinCounter);

        reader.close();
    }

    public void buildDiseasePanels(Path panelPath) throws IOException {
        ObjectReader mapperReader = new ObjectMapper().reader(DiseasePanel.class);

        // Get CSV file writers
        PrintWriter pwDiseasePanelNode = csv.getCsvWriters().get(Node.Type.DISEASE_PANEL.toString());
        PrintWriter pwPanelGeneNode = csv.getCsvWriters().get(Node.Type.PANEL_GENE.toString());

        BufferedReader reader = FileUtils.newBufferedReader(panelPath);

        String jsonDiseasePanel = reader.readLine();
        while (jsonDiseasePanel != null) {
            DiseasePanel diseasePanel = mapperReader.readValue(jsonDiseasePanel);

            // Create disease panel node and save CSV file
            Node diseasePanelNode = NodeBuilder.newNode(csv.getAndIncUid(), diseasePanel);
            pwDiseasePanelNode.println(csv.nodeLine(diseasePanelNode));

            for (DiseasePanel.GenePanel panelGene : diseasePanel.getGenes()) {
                // Create panel gene node and save CSV file
                Node panelGeneNode = NodeBuilder.newNode(csv.getAndIncUid(), panelGene);
                pwPanelGeneNode.println(csv.nodeLine(panelGeneNode));

                csv.getCsvWriters().get(Relation.Type.HAS___DISEASE_PANEL___PANEL_GENE.toString()).println(diseasePanelNode.getUid()
                        + CsvInfo.SEPARATOR + panelGeneNode.getUid());

                if (StringUtils.isNotEmpty(panelGene.getId())) {
                    Long geneUid = csv.getGeneUid(panelGene.getId());
                    if (geneUid == null) {
                        geneUid = csv.getGeneUid(panelGene.getName());
                    }

//                    Long geneUid = processGene(panelGene.getId(), panelGene.getName());
                    if (geneUid != null) {
                        // Add relation to CSV file
                        csv.getCsvWriters().get(Relation.Type.ANNOTATION___GENE___PANEL_GENE.toString()).println(geneUid + CsvInfo.SEPARATOR
                                + panelGeneNode.getUid());
                    } else {
                        String msg = "Not found!!! Gene " + panelGene.getId() + " (" + panelGene.getName() + ") from disease panel "
                                + diseasePanel.getId();
                        logger.warn(msg);
//                        System.out.println(msg);
                    }
                }
            }

            // Next panel
            jsonDiseasePanel = reader.readLine();
        }
    }

    private List<String> readSampleIds(String variantFile) {
        List<String> sampleIds = new ArrayList<>();

        File metaFile = new File(variantFile + ".meta.json");
        if (!metaFile.exists()) {
            metaFile = new File(variantFile + ".meta.json.gz");
        }
        if (!metaFile.exists()) {
            return sampleIds;
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
            return sampleIds;
        }

        if (CollectionUtils.isNotEmpty(variantMetadata.getStudies())) {
            // IMPORTANT: it considers only the first study
            VariantStudyMetadata variantStudyMetadata = variantMetadata.getStudies().get(0);

            // Get sample attributes
            for (Individual individual : variantStudyMetadata.getIndividuals()) {
                if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                    for (Sample sample : individual.getSamples()) {
                        sampleIds.add(sample.getId());
                    }
                }
            }
        }

        return sampleIds;
    }

    public CsvInfo getCsvInfo() {
        return csv;
    }

    private void createVariantObjectNode(Variant variant, Node variantNode) throws IOException {
        // Create variant object node
        Node variantObjectNode = new Node(csv.getAndIncUid(), variant.toString(), variant.getId(), Node.Type.VARIANT_OBJECT);

        // Studies
        String value = "";
        if (CollectionUtils.isNotEmpty(variant.getStudies())) {
            value = Utils.compress(variant.getStudies(), mapper);
            variant.setStudies(Collections.emptyList());
        }
        variantObjectNode.addAttribute("studies", value);

        if (variant.getAnnotation() != null) {
            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                value = Utils.compress(variant.getAnnotation().getConsequenceTypes(), mapper);
                variant.getAnnotation().setConsequenceTypes(Collections.emptyList());
            }
            variantObjectNode.addAttribute("consequenceTypes", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getXrefs())) {
                value = Utils.compress(variant.getAnnotation().getXrefs(), mapper);
                variant.getAnnotation().setXrefs(Collections.emptyList());
            }
            variantObjectNode.addAttribute("xrefs", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                value = Utils.compress(variant.getAnnotation().getPopulationFrequencies(), mapper);
                variant.getAnnotation().setPopulationFrequencies(Collections.emptyList());
            }
            variantObjectNode.addAttribute("populationFrequencies", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                value = Utils.compress(variant.getAnnotation().getConservation(), mapper);
                variant.getAnnotation().setConservation(Collections.emptyList());
            }
            variantObjectNode.addAttribute("conservation", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getGeneExpression())) {
                value = Utils.compress(variant.getAnnotation().getGeneExpression(), mapper);
                variant.getAnnotation().setGeneExpression(Collections.emptyList());
            }
            variantObjectNode.addAttribute("geneExpression", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getGeneTraitAssociation())) {
                value = Utils.compress(variant.getAnnotation().getGeneTraitAssociation(), mapper);
                variant.getAnnotation().setGeneTraitAssociation(Collections.emptyList());
            }
            variantObjectNode.addAttribute("geneTraitAssociation", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getGeneDrugInteraction())) {
                value = Utils.compress(variant.getAnnotation().getGeneDrugInteraction(), mapper);
                variant.getAnnotation().setGeneDrugInteraction(Collections.emptyList());
            }
            variantObjectNode.addAttribute("geneDrugInteraction", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                value = Utils.compress(variant.getAnnotation().getTraitAssociation(), mapper);
                variant.getAnnotation().setTraitAssociation(Collections.emptyList());
            }
            variantObjectNode.addAttribute("traitAssociation", value);

            value = "";
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                value = Utils.compress(variant.getAnnotation().getFunctionalScore(), mapper);
                variant.getAnnotation().setFunctionalScore(Collections.emptyList());
            }
            variantObjectNode.addAttribute("functionalScore", value);
        }

        variantObjectNode.addAttribute("core", Utils.compress(variant, mapper));

        // Write node to CSV file
        PrintWriter pw = csv.getCsvWriters().get(Node.Type.VARIANT_OBJECT.toString());
        pw.println(csv.nodeLine(variantObjectNode));


        // Create relation to gene node and write
        pw = csv.getCsvWriters().get(Relation.Type.VARIANT__VARIANT_OBJECT.toString());
        pw.println(variantNode.getUid() + CsvInfo.SEPARATOR + variantObjectNode.getUid());
    }

    public List<String> getAdditionalVariantFiles() {
        return additionalVariantFiles;
    }

    public Builder setAdditionalVariantFiles(List<String> additionalVariantFiles) {
        this.additionalVariantFiles = additionalVariantFiles;
        return this;
    }

    public List<String> getAdditionalNeworkFiles() {
        return additionalNeworkFiles;
    }

    public Builder setAdditionalNeworkFiles(List<String> additionalNeworkFiles) {
        this.additionalNeworkFiles = additionalNeworkFiles;
        return this;
    }
}
