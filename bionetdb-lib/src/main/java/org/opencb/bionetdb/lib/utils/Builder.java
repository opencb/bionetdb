package org.opencb.bionetdb.lib.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v202003jaxb.*;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.*;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.opencb.bionetdb.lib.db.Neo4jBioPaxBuilder;
import org.opencb.bionetdb.lib.utils.cache.GeneCache;
import org.opencb.bionetdb.lib.utils.cache.ProteinCache;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class Builder {
    public static final Object GENE_FILENAME = "gene.json";
    public static final Object GENE_DBNAME = "gene.rocksdb";

    public static final Object PANEL_FILENAME = "panels.json";

    public static final Object PROTEIN_FILENAME = "protein.json";
    public static final Object PROTEIN_DBNAME = "protein.rocksdb";

    public static final Object MIRNA_FILENAME = "mirna.csv";
    public static final Object MIRNA_DBNAME = "mirna.rocksdb";

    public static final Object NETWORK_FILENAME = "Homo_sapiens.owl";

    public static final Object CLINICAL_VARIANT_FILENAME = "clinical_variants.json";

    private CsvInfo csv;
    private Path inputPath;
    private Path outputPath;
    private Map<String, Set<String>> filters;
    private ObjectMapper mapper;

    protected static Logger logger;

    public void build() throws IOException {
        long start;

        // Open CSV files
        csv.openCSVFiles();

        long geneBuildTime = 0;
        long proteinBuildTime = 0;
        long genePanelBuildTime = 0;
//            long miRnaIndexingTime = 0;
        long bioPaxBuildTime = 0;
        long clinvarBuildTime = 0;


        // Check input files
        File geneFile = new File(inputPath + "/" + GENE_FILENAME);
        if (!geneFile.exists()) {
            geneFile = new File(inputPath + "/" + GENE_FILENAME + ".gz");
            FileUtils.checkFile(geneFile.toPath());
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

        // Processing genes
        logger.info("Processing genes...");
        start = System.currentTimeMillis();
        buildGenes(geneFile.toPath());
        geneBuildTime = (System.currentTimeMillis() - start) / 1000;
        logger.info("Gene processing done in {} s", geneBuildTime);

        // Processing proteins
        logger.info("Processing proteins...");
        start = System.currentTimeMillis();
        buildProteins(proteinFile.toPath());
        proteinBuildTime = (System.currentTimeMillis() - start) / 1000;
        logger.info("Protein processing done in {} s", proteinBuildTime);

        // Gene panels support
        logger.info("Processing gene panels...");
        start = System.currentTimeMillis();
        buildGenePanels(panelFile.toPath());
//            importer.addGenePanels(Paths.get(inputPath + "/" + Neo4jCsvImporter.PANEL_DIRNAME), outputPath);
        genePanelBuildTime = (System.currentTimeMillis() - start) / 1000;
        logger.info("Gene panel processing done in {} s", genePanelBuildTime);


        // Procesing BioPAX file
        BioPAXProcessing biopaxProcessing = new BioPAXProcessing(this);
        Neo4jBioPaxBuilder bioPAXImporter = new Neo4jBioPaxBuilder(csv, filters, biopaxProcessing);
        start = System.currentTimeMillis();
        bioPAXImporter.build(networkFile.toPath());
        biopaxProcessing.post();
        bioPaxBuildTime = (System.currentTimeMillis() - start) / 1000;


//        // Processing clinical variants
//        logger.info("Processing clinical variants...");
//        start = System.currentTimeMillis();
//        buildClinicalVariants(clinicalVariantFile.toPath());
//        clinvarBuildTime = (System.currentTimeMillis() - start) / 1000;
//        logger.info("Processing clinical variants done in {} s", clinvarBuildTime);

        // Close CSV files
        csv.close();

        logger.info("Gene build time: {} s", geneBuildTime);
        logger.info("Protein build time: {} s", proteinBuildTime);
        logger.info("Gene panel build time: {} s", genePanelBuildTime);
        logger.info("BioPAX build time: {} s", bioPaxBuildTime);
        logger.info("Clinical variant build time: {} s", clinvarBuildTime);
    }


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


            /*
            // Post-process miRNA nodes
            logger.info("Post-processing {} miRNA nodes", rnaNodes.size());
            pwNode = csv.getCsvWriters().get(Node.Type.RNA.toString());
            PrintWriter pwMiRna = csv.getCsvWriters().get(Node.Type.MIRNA.toString());
            PrintWriter pwMiRnaTargetRel = csv.getCsvWriters().get(
                    CsvInfo.BioPAXRelation.TARGET_GENE___MIRNA___GENE.toString());
            pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.IS___RNA___MIRNA.toString());
            for (Node node: rnaNodes) {
                List<String> miRnaInfo = getMiRnaInfo(node.getName());
                for (String info: miRnaInfo) {
                    String[] fields = info.split(":");
                    String miRnaId = fields[0];
                    String targetGene = fields[1];
                    String evidence = fields[2];

                    Long miRnaUid = csv.getLong(miRnaId);
                    if (miRnaUid == null) {
                        Node miRnaNode = new Node(csv.getAndIncUid(), miRnaId, miRnaId, Node.Type.MIRNA);
                        pwMiRna.println(csv.nodeLine(miRnaNode));

                        // Save the miRNA node uid
                        miRnaUid = miRnaNode.getUid();
                        csv.putLong(miRnaId, miRnaUid);
                    }

                    // Process target gene
                    Long geneUid = builder.processGene(targetGene, targetGene);
                    if (geneUid != null) {
                        if (csv.getLong(miRnaUid + "." + geneUid) == null) {
                            // Write mirna-target gene relation
                            pwMiRnaTargetRel.println(miRnaUid + "," + geneUid + "," + evidence);
                            csv.putLong(miRnaUid + "." + geneUid, 1);
                        }
                    }

                    // Write rna-mirna relation
                    pwRel.println(node.getUid() + CsvInfo.SEPARATOR + miRnaUid);
                }
                // Write RNA node
                pwNode.println(builder.getCsvInfo().nodeLine(node));
            }
*/
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

        if (proteinUid == 3196753) {
            System.out.println("processProtein: proteinUid = " + proteinUid);
            System.out.println("processProtein: proteinId = " + proteinId);
            System.exit(-1);
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

        // Gene object node management
        try {
            // Create gene object node and write
            Node geneObjectNode = new Node(csv.getAndIncUid(), geneNode.getId(), geneNode.getName(), Node.Type.GENE_OBJECT);
            geneObjectNode.addAttribute("object", Utils.compress(gene, mapper));
            PrintWriter pw = csv.getCsvWriters().get(Node.Type.GENE_OBJECT.toString());
            pw.println(csv.nodeLine(geneObjectNode));

            // Create relation to gene node and write
            pw = csv.getCsvWriters().get(Relation.Type.GENE__GENE_OBJECT.toString());
            pw.println(uid + CsvInfo.SEPARATOR + geneObjectNode.getUid());
        } catch (IOException e) {
            logger.warn("Unable to create GZ JSON object for gene '{}': {}", gene.getId(), e.getMessage());
        }

        // Model transcripts
        if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
            pwRel = csv.getCsvWriters().get(Relation.Type.GENE__TRANSCRIPT.toString());
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
                pwRel = csv.getCsvWriters().get(Relation.Type.GENE__DRUG.toString());
                for (GeneDrugInteraction drug : gene.getAnnotation().getDrugs()) {
                    Long drugUid = csv.getLong(drug.getDrugName(), Node.Type.DRUG.name());
                    if (drugUid == null) {
                        n = NodeBuilder.newNode(csv.getAndIncUid(), drug);
                        updateCSVFiles(uid, n, Relation.Type.GENE__DRUG.toString());
                        // Save drug and gene-drug UIDs
                        csv.putLong(drug.getDrugName(), Node.Type.DRUG.name(), n.getUid());
                        csv.putLong(uid + "." + n.getUid(), Node.Type.DRUG.name(), 1);
                    } else {
                        String key = uid + "." + drugUid;
                        if (csv.getLong(key, Node.Type.DRUG.name()) == null) {
                            // Create gene-drug relation
                            pwRel.println(csv.relationLine(uid, drugUid));
                            // Save relation to avoid duplicated ones
                            csv.putLong(key, Node.Type.DRUG.name(), 1);
                        }
                    }
                }
            }

            // Model diseases
            if (CollectionUtils.isNotEmpty(gene.getAnnotation().getDiseases())) {
                pwRel = csv.getCsvWriters().get(Relation.Type.GENE__DISEASE.toString());
                for (GeneTraitAssociation disease : gene.getAnnotation().getDiseases()) {
                    String diseaseId = disease.getId() + "_" + (disease.getHpo() != null ? disease.getHpo() : "");
                    Long diseaseUid = csv.getLong(diseaseId, Node.Type.DISEASE.name());
                    if (diseaseUid == null) {
                        n = NodeBuilder.newNode(csv.getAndIncUid(), disease);
                        updateCSVFiles(uid, n, Relation.Type.GENE__DISEASE.toString());

                        csv.putLong(diseaseId, Node.Type.DISEASE.name(), n.getUid());
                    } else {
                        // create gene-disease relation
                        pwRel.println(csv.relationLine(uid, diseaseUid));
                    }
                }
            }

            // Model constraint
            if (CollectionUtils.isNotEmpty(gene.getAnnotation().getConstraints())) {
                for (Constraint constraint : gene.getAnnotation().getConstraints()) {
                    n = NodeBuilder.newNode(csv.getAndIncUid(), constraint);
                    // Write constraint node and gene-constraint relation
                    updateCSVFiles(uid, n, Relation.Type.GENE__CONSTRAINT.toString());
                }
            }
        }

        // Xrefs
        PrintWriter pwXref = csv.getCsvWriters().get(Node.Type.XREF.toString());
        pwRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.XREF___GENE___XREF.toString());
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

        // Protein object node management
        try {
            // Create gene object node and write
            Node proteinObjectNode = new Node(csv.getAndIncUid(), proteinNode.getId(), proteinNode.getName(), Node.Type.PROTEIN_OBJECT);
            proteinObjectNode.addAttribute("object", Utils.compress(protein, mapper));
            pw = csv.getCsvWriters().get(Node.Type.PROTEIN_OBJECT.toString());
            pw.println(csv.nodeLine(proteinObjectNode));

            // Create relation to gene node and write
            pw = csv.getCsvWriters().get(Relation.Type.PROTEIN__PROTEIN_OBJECT.toString());
            pw.println(uid + CsvInfo.SEPARATOR + proteinObjectNode.getUid());
        } catch (IOException e) {
            logger.warn("Unable to create GZ JSON object for protein '{}': {}", proteinNode.getId(), e.getMessage());
        }

        // Model protein keywords
        if (CollectionUtils.isNotEmpty(protein.getKeyword())) {
            pw = csv.getCsvWriters().get(Relation.Type.PROTEIN__PROTEIN_KEYWORD.toString());
            for (KeywordType keyword: protein.getKeyword()) {
                Long kwUid = csv.getLong(keyword.getId(), Node.Type.PROTEIN_KEYWORD.name());
                if (kwUid == null) {
                    n = new Node(csv.getAndIncUid(), keyword.getId(), keyword.getValue(), Node.Type.PROTEIN_KEYWORD);
                    updateCSVFiles(uid, n, Relation.Type.PROTEIN__PROTEIN_KEYWORD.toString());
                    csv.putLong(keyword.getId(), Node.Type.PROTEIN_KEYWORD.name(), n.getUid());
                } else {
                    // Create protein - protein keyword relation
                    pw.println(csv.relationLine(uid, kwUid));
                }
            }
        }

        // Model protein features
        if (CollectionUtils.isNotEmpty(protein.getFeature())) {
            for (FeatureType feature: protein.getFeature()) {
                n = NodeBuilder.newNode(csv.getAndIncUid(), feature);
                updateCSVFiles(uid, n, Relation.Type.PROTEIN__PROTEIN_FEATURE.toString());
            }
        }

        // Model Xrefs
        if (CollectionUtils.isNotEmpty(protein.getDbReference())) {
            PrintWriter pwXref = csv.getCsvWriters().get(Node.Type.XREF.toString());
            pw = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.XREF___PROTEIN___XREF.toString());
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

        // Get protein
        Node n;
        if (CollectionUtils.isNotEmpty(transcript.getXrefs())) {
            String proteinId;
            Entry protein;
            for (Xref xref: transcript.getXrefs()) {
                proteinId = csv.getProteinCache().getPrimaryId(xref.getId());
                if (proteinId != null) {
                    Long proteinUid = csv.getLong(proteinId, Node.Type.PROTEIN.name());
                    if (proteinUid == null) {
                        protein = csv.getProtein(proteinId);
                        if (protein != null) {
                            // Create protein node and write the CSV file
                            Node proteinNode = createProteinNode(protein);
                            csv.getCsvWriters().get(Node.Type.PROTEIN.toString()).println(csv.nodeLine(proteinNode));
                            proteinUid = proteinNode.getUid();

                            // Save protein UID
                            csv.saveProteinUid(proteinId, proteinUid);
                        } else {
                            logger.info("Protein not found for ID {}", proteinId);
                        }
                    }

                    // Write transcript-protein relation
                    csv.getCsvWriters().get(Relation.Type.TRANSCRIPT__PROTEIN.toString()).println(uid + CsvInfo.SEPARATOR + proteinUid);
                    break;
                }
            }
        }

        // Model exon
        if (CollectionUtils.isNotEmpty(transcript.getExons())) {
            PrintWriter pwRel = csv.getCsvWriters().get(Relation.Type.TRANSCRIPT__EXON.toString());
            for (Exon exon : transcript.getExons()) {
                Long exonUid = csv.getLong(exon.getId(), Node.Type.EXON.name());
                if (exonUid == null) {
                    n = createExonNode(exon);
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
                updateCSVFiles(uid, n, Relation.Type.TRANSCRIPT__TFBS.toString());
            }
        }

        if (transcript.getAnnotation() != null) {
            // Model constraint
            if (CollectionUtils.isNotEmpty(transcript.getAnnotation().getConstraints())) {
                for (Constraint constraint : transcript.getAnnotation().getConstraints()) {
                    n = NodeBuilder.newNode(csv.getAndIncUid(), constraint);
                    // Write constraint node and transcript-constraint relation
                    updateCSVFiles(uid, n, Relation.Type.TRANSCRIPT__CONSTRAINT.toString());
                }
            }

            // Model feature ontology term annotation
            if (CollectionUtils.isNotEmpty(transcript.getAnnotation().getOntologies())) {
                PrintWriter pwRel = csv.getCsvWriters().get(Relation.Type.TRANSCRIPT__FEATURE_ONTOLOGY_TERM_ANNOTATION.toString());
                for (FeatureOntologyTermAnnotation ontology : transcript.getAnnotation().getOntologies()) {
                    Long ontologyUid = csv.getLong(ontology.getId(), Node.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION.name());
                    if (ontologyUid == null) {
                        n = createFeatureOntologyTermAnnotationNode(ontology);
                        ontologyUid = n.getUid();
                    }
                    // Add relation transcript-ontology to CSV file
                    pwRel.println(uid + CsvInfo.SEPARATOR + ontologyUid);
                }
            }
        }

        return node;
    }

    private Node createExonNode(Exon exon) {
        return createExonNode(exon, csv.getAndIncUid());
    }

    private Node createExonNode(Exon exon, Long uid) {
        // Create exon node and save UId and wrrite CSV file
        Node node = NodeBuilder.newNode(uid, exon);
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
                updateCSVFiles(uid, n, Relation.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION__ANNOTATION_EVIDENCE.toString());
            }
        }

        return node;
    }

    private Long processVariant(Variant variant) throws IOException {
        Node variantNode = null;

        Long variantUid = csv.getLong(variant.toStringSimple(), Node.Type.VARIANT.name());
        if (variantUid == null) {
            variantNode = createVariantNode(variant);
            variantUid = variantNode.getUid();
            csv.putLong(variant.toStringSimple(), Node.Type.VARIANT.name(), variantUid);
        }

        // Process sample info
//        processSampleInfo(variant, variantUid);

        // Check if we have to update the VARIANT_OBJECT.csv file
        if (variantNode != null) {
            createVariantObjectNode(variant, variantNode);
        }

        return variantUid;
    }

    private Node createVariantNode(Variant variant) {
        return createVariantNode(variant, csv.getAndIncUid());
    }

    private Node createVariantNode(Variant variant, Long varUid) {
        Node varNode = NodeBuilder.newNode(varUid, variant);
        PrintWriter pw = csv.getCsvWriters().get(Node.Type.VARIANT.toString());
        pw.println(csv.nodeLine(varNode));

        // Annotation management
        if (variant.getAnnotation() != null) {
            // Consequence types

            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                // Consequence type nodes
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    Node ctNode = NodeBuilder.newNode(csv.getAndIncUid(), ct);
                    updateCSVFiles(varUid, ctNode, Relation.Type.VARIANT__CONSEQUENCE_TYPE.toString());

                    // Gene
                    Long geneUid = processGene(ct.getEnsemblGeneId(), ct.getGeneName());
                    if (geneUid != null) {
                        // Relation: consequence type - gene
                        pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__GENE.toString());
                        pw.println(csv.relationLine(ctNode.getUid(), geneUid));
                    }

                    // Transcript
                    Long transcriptUid = processTranscript(ct.getEnsemblTranscriptId());
                    if (transcriptUid != null) {
                        if (geneUid != null) {
                            csv.getCsvWriters().get(Relation.Type.GENE__TRANSCRIPT.toString()).println(csv.relationLine(
                                    geneUid, transcriptUid));
                        }

                        // Relation: consequence type - transcript
                        pw = csv.getCsvWriters().get(Relation.Type.CONSEQUENCE_TYPE__TRANSCRIPT.toString());
                        pw.println(csv.relationLine(ctNode.getUid(), transcriptUid));
                    } else {
                        if (geneUid != null) {
                            logger.info("Transcript UID is null for {} (gene {}), maybe version mismatch!!"
                                            + " Gene vs variant annotation!!!", ct.getEnsemblTranscriptId(),
                                    ct.getEnsemblGeneId());
                        }
                    }

                    // SO
                    if (CollectionUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                            String soId = so.getAccession();
                            if (soId != null) {
                                Long soUid = csv.getLong(soId, Node.Type.SO.name());
                                if (soUid == null) {
                                    Node soNode = new Node(csv.getAndIncUid(), so.getAccession(), so.getName(), Node.Type.SO);
                                    updateCSVFiles(ctNode.getUid(), soNode, Relation.Type.CONSEQUENCE_TYPE__SO.toString());
                                    csv.putLong(soId, Node.Type.SO.name(), soNode.getUid());
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
                        if (CollectionUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                            for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                Node scoreNode = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.SUBSTITUTION_SCORE);
                                updateCSVFiles(pVANode.getUid(), scoreNode,
                                        Relation.Type.PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE.toString());
                            }
                        }
                    }
                }
            }

            Node node;

            // Population frequencies
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                    // Population frequency node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), popFreq);
                    updateCSVFiles(varUid, node, Relation.Type.VARIANT__POPULATION_FREQUENCY.toString());
                }
            }

            // Conservation values
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                for (Score score: variant.getAnnotation().getConservation()) {
                    // Conservation node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.CONSERVATION);
                    updateCSVFiles(varUid, node, Relation.Type.VARIANT__CONSERVATION.toString());
                }
            }

            // Trait associations
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                for (EvidenceEntry evidence: variant.getAnnotation().getTraitAssociation()) {
                    // Trait association node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), evidence, Node.Type.TRAIT_ASSOCIATION);
                    updateCSVFiles(varUid, node, Relation.Type.VARIANT__TRAIT_ASSOCIATION.toString());
                }
            }

            // Functional scores
            if (CollectionUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    // Functional score node
                    node = NodeBuilder.newNode(csv.getAndIncUid(), score, Node.Type.FUNCTIONAL_SCORE);
                    updateCSVFiles(varUid, node, Relation.Type.VARIANT__FUNCTIONAL_SCORE.toString());
                }
            }
        }

        return varNode;
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


    private void buildClinicalVariants(Path path) throws IOException {
        // Reading file line by line, each line a JSON object
        BufferedReader reader = FileUtils.newBufferedReader(path);

//        File metaFile = new File(file.getAbsoluteFile() + ".meta.json");
//        if (metaFile.exists()) {
//            csv.openMetadataFile(metaFile);
//        } else {
//            metaFile = new File(file.getAbsoluteFile() + ".meta.json.gz");
//            if (metaFile.exists()) {
//                csv.openMetadataFile(metaFile);
//            }
//        }

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

//    private void processSampleInfo(Variant variant, Long variantUid) {
//        String variantId = variant.toString();
//
//        if (CollectionUtils.isNotEmpty(variant.getStudies())) {
//            PrintWriter pw;
//
//            // Only one single study is supported
//            StudyEntry studyEntry = variant.getStudies().get(0);
//
//            if (CollectionUtils.isNotEmpty(studyEntry.getFiles())) {
//                // INFO management: FILTER, QUAL and info fields
//                String fileId = studyEntry.getFiles().get(0).getFileId();
//                String infoId = variantId + "_" + fileId;
//                Long infoUid = csv.getLong(infoId);
//                if (infoUid == null) {
//                    // Variant file info node
//                    infoUid = csv.getAndIncUid();
//                    pw = csv.getCsvWriters().get(Node.Type.VARIANT_FILE_INFO.toString());
//                    pw.println(variantInfoLine(infoUid, studyEntry));
//                }
//                Long fileUid = csv.getLong(fileId);
//                if (fileUid != null) {
//                    pw = csv.getCsvWriters().get(Relation.Type.VARIANT_FILE_INFO__FILE.toString());
//                    pw.println(infoUid + CsvInfo.SEPARATOR + fileUid);
//                }
//
//                // FORMAT: GT and format attributes
//                for (int i = 0; i < studyEntry.getSamples().size(); i++) {
//                    String sampleName = CollectionUtils.isEmpty(csv.getSampleNames())
//                            ? "sample" + (i + 1)
//                            : csv.getSampleNames().get(i);
//                    Long sampleUid = csv.getLong(sampleName);
//                    if (sampleUid == null) {
//                        sampleUid = csv.getAndIncUid();
//                        csv.getCsvWriters().get(Node.Type.SAMPLE.toString())
//                                .println(sampleUid + CsvInfo.SEPARATOR + sampleName + CsvInfo.SEPARATOR + sampleName);
//                        csv.putLong(sampleName, sampleUid);
//                    }
//                    // Variant call node
//                    Long formatUid = csv.getAndIncUid();
//                    pw = csv.getCsvWriters().get(Node.Type.VARIANT_CALL.toString());
//                    pw.println(variantFormatLine(formatUid, studyEntry, i));
//
//                    // Relation: variant - variant call
//                    pw = csv.getCsvWriters().get(Relation.Type.VARIANT__VARIANT_CALL.toString());
//                    pw.println(csv.relationLine(variantUid, formatUid));
//
//                    // Relation: sample - variant call
//                    pw = csv.getCsvWriters().get(Relation.Type.SAMPLE__VARIANT_CALL.toString());
//                    pw.println(csv.relationLine(sampleUid, formatUid));
//
//                    // Relation: variant call - variant file info
//                    pw = csv.getCsvWriters().get(Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString());
//                    pw.println(csv.relationLine(formatUid, infoUid));
//                }
//            }
//        }
//    }

//    private String variantInfoLine(long infoUid, StudyEntry studyEntry) {
//        StringBuilder sb = new StringBuilder("" + infoUid);
//        Map<String, String> fileData = studyEntry.getFiles().get(0).getData();
//
//        Iterator<String> iterator = csv.getInfoFields().iterator();
//        while (iterator.hasNext()) {
//            String infoName = iterator.next();
//            if (sb.length() > 0) {
//                sb.append(CsvInfo.SEPARATOR);
//            }
//            if (fileData.containsKey(infoName)) {
//                sb.append(fileData.get(infoName));
//            } else {
//                sb.append(CsvInfo.MISSING_VALUE);
//            }
//        }
//        return sb.toString();
//    }

//    private String variantFormatLine(Long formatUid, StudyEntry studyEntry, int index) {
//        StringBuilder sb = new StringBuilder("" + formatUid);
//
//        Iterator<String> iterator = csv.getFormatFields().iterator();
//        while (iterator.hasNext()) {
//            String formatName = iterator.next();
//            if (sb.length() > 0) {
//                sb.append(CsvInfo.SEPARATOR);
//            }
////            if (studyEntry.getFormatPositions().containsKey(formatName)) {
////                sb.append(studyEntry.getSampleData(index).get(studyEntry.getFormatPositions()
////                        .get(formatName)));
////            } else {
//            sb.append(CsvInfo.MISSING_VALUE);
////            }
//        }
//        return sb.toString();
//    }

    //-------------------------------------------------------------------------


//    private void addClinicalAnalysisJsonFile(File file) throws IOException {
//        // Reading file line by line, each line a JSON object
//        BufferedReader reader;
//        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
//
//        // TODO: how to get metadata from clinical analysis (format field, sample names,...)
//        boolean done = false;
//
//        long counter = 0;
//        logger.info("Processing JSON file {}", file.getPath());
//        reader = FileUtils.newBufferedReader(file.toPath());
//        String line = reader.readLine();
//        while (line != null) {
//            ClinicalAnalysis clinicalAnalysis = mapper.readValue(line, ClinicalAnalysis.class);
//            if (!done) {
//                done = processMetadataFromClinicalAnalysis(clinicalAnalysis);
//            }
//            processClinicalAnalysis(clinicalAnalysis);
//
//            // read next line
//            line = reader.readLine();
//            if (++counter % 5000 == 0) {
//                logger.info("Parsing {} clinical analsysis...", counter);
//            }
//        }
//        reader.close();
//        logger.info("Parsed {} clinical analysis from {}. Done!!!", counter, file.toString());
//    }
//
//    private boolean processMetadataFromClinicalAnalysis(ClinicalAnalysis clinicalAnalysis) {
//        boolean done = false;
//        int counter = 0;
//
//        Set<String> formats = new HashSet<>();
//        Set<String> info = new HashSet<>();
//
//        if (org.apache.commons.collections.CollectionUtils.isNotEmpty(clinicalAnalysis.getInterpretations())) {
//            for (Interpretation interpretation : clinicalAnalysis.getInterpretations()) {
//                if (org.apache.commons.collections.CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
//                    for (ReportedVariant variant : interpretation.getPrimaryFindings()) {
//                        if (org.apache.commons.collections.CollectionUtils.isNotEmpty(variant.getStudies())) {
//                            for (StudyEntry study : variant.getStudies()) {
//                                // Info fields
//                                for (FileEntry file : study.getFiles()) {
//                                    if (MapUtils.isNotEmpty(file.getAttributes())) {
//                                        info.addAll(file.getAttributes().keySet());
//                                    }
//                                }
//
//                                // Format fields
//                                if (org.apache.commons.collections.CollectionUtils.isNotEmpty(study.getFormat())) {
//                                    formats.addAll(study.getFormat());
//                                }
//                            }
//                        }
//                        counter++;
//                        if (counter > 2) {
//                            break;
//                        }
//                    }
//                }
//                if (counter > 2) {
//                    break;
//                }
//            }
//        }
//
//        if (org.apache.commons.collections.CollectionUtils.isNotEmpty(info)
//                && org.apache.commons.collections.CollectionUtils.isNotEmpty(formats)) {
//
//            // Variant call
//            String strType;
//            List<String> attrs = new ArrayList<>();
//            attrs.add("variantCallId");
//            Iterator<String> iterator = formats.iterator();
//            while (iterator.hasNext()) {
//                attrs.add(iterator.next());
//            }
//            strType = Node.Type.VARIANT_CALL.toString();
//            Map<String, List<String>> nodeAttributes = csv.getNodeAttributes();
//            nodeAttributes.put(strType, attrs);
//            csv.getCsvWriters().get(strType).println(csv.getNodeHeaderLine(attrs));
//            strType = Relation.Type.VARIANT__VARIANT_CALL.toString();
//            csv.getCsvWriters().get(strType).println(csv.getRelationHeaderLine(strType));
//            strType = Relation.Type.SAMPLE__VARIANT_CALL.toString();
//            csv.getCsvWriters().get(strType).println(csv.getRelationHeaderLine(strType));
//
//            // Variant file info
//            attrs = new ArrayList<>();
//            attrs.add("variantFileInfoId");
//            iterator = info.iterator();
//            while (iterator.hasNext()) {
//                attrs.add(iterator.next());
//            }
//            strType = Node.Type.VARIANT_FILE_INFO.toString();
//            nodeAttributes.put(strType, attrs);
//            csv.getCsvWriters().get(strType).println(csv.getNodeHeaderLine(attrs));
//            strType = Relation.Type.VARIANT_CALL__VARIANT_FILE_INFO.toString();
//            csv.getCsvWriters().get(strType).println(csv.getRelationHeaderLine(strType));
//            strType = Relation.Type.VARIANT_FILE_INFO__FILE.toString();
//            csv.getCsvWriters().get(strType).println(csv.getRelationHeaderLine(strType));
//
//            done = true;
//        }
//        return done;
//    }

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
                if (org.apache.commons.lang3.StringUtils.isNotEmpty(gene.getName())) {
                    geneCache.saveXref(gene.getName(), geneId);
                }

                if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                    for (Transcript transcr : gene.getTranscripts()) {
                        if (CollectionUtils.isNotEmpty(transcr.getXrefs())) {
                            for (Xref xref: transcr.getXrefs()) {
                                if (org.apache.commons.lang3.StringUtils.isNotEmpty(xref.getId())) {
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
                                pw.println(geneUid + CsvInfo.SEPARATOR + matureUid + CsvInfo.SEPARATOR + target.getExperiment()
                                        + CsvInfo.SEPARATOR + target.getEvidence() + CsvInfo.SEPARATOR + target.getPubmed());
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
                        if (org.apache.commons.lang3.StringUtils.isNotEmpty(shortName.getValue())) {
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
                    for (DbReferenceType dbRef: protein.getDbReference()) {
                        proteinCache.saveXref(dbRef.getId(), proteinAcc);
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

    public void buildGenePanels(Path panelPath) throws IOException {
        File[] panelFiles = panelPath.toFile().listFiles();

        ObjectReader mapperReader = new ObjectMapper().reader(DiseasePanel.class);

        // Get CSV file writers
        PrintWriter pwNode = csv.getCsvWriters().get(Node.Type.PANEL.toString());
        PrintWriter pwRel = csv.getCsvWriters().get(Relation.Type.PANEL__GENE.toString());

        BufferedReader reader = FileUtils.newBufferedReader(panelPath);

        String jsonPanel = reader.readLine();
        while (jsonPanel != null) {
            DiseasePanel panel = mapperReader.readValue(jsonPanel);

            // Create node and save CSV file
            Node node = NodeBuilder.newNode(csv.getAndIncUid(), panel);
            pwNode.println(csv.nodeLine(node));

            for (DiseasePanel.GenePanel gene : panel.getGenes()) {
                if (StringUtils.isNotEmpty(gene.getId())) {
                    Long geneUid = processGene(gene.getId(), gene.getName());
                    if (geneUid != null) {
                        // Add relation to CSV file
                        pwRel.println(node.getUid() + CsvInfo.SEPARATOR + geneUid);
                    }
                }
            }

            // Next panel
            jsonPanel = reader.readLine();
        }
    }

//    public void indexingMiRnas(Path miRnaPath, Path indexPath, boolean toImport) throws IOException {
//        csv.indexingMiRnas(miRnaPath, indexPath);
//
//        if (toImport) {
//            // Import miRNAs and genes
//            PrintWriter pwMiRna = csv.getCsvWriters().get(Node.Type.MIRNA.toString());
//            PrintWriter pwMiRnaTargetRel = csv.getCsvWriters().get(CsvInfo.BioPAXRelation.TARGET_GENE___MIRNA___GENE
//                    .toString());
//
//            RocksIterator rocksIterator = csv.getMiRnaRocksDb().newIterator();
//            rocksIterator.seekToFirst();
//            while (rocksIterator.isValid()) {
//                String miRnaId = new String(rocksIterator.key());
//                String miRnaInfo = new String(rocksIterator.value());
//
//                Long miRnaUid = csv.getLong(miRnaId);
//                if (miRnaUid == null) {
//                    Node miRnaNode = new Node(csv.getAndIncUid(), miRnaId, miRnaId, Node.Type.MIRNA);
//                    pwMiRna.println(csv.nodeLine(miRnaNode));
//
//                    // Save the miRNA node uid
//                    miRnaUid = miRnaNode.getUid();
//                    csv.putLong(miRnaId, miRnaUid);
//                }
//
//                String[] fields = miRnaInfo.split("::");
//                for (int i = 0; i < fields.length; i++) {
//                    String[] subFields = fields[i].split(":");
//
//                    // Process target gene
//                    Long geneUid = processGene(subFields[0], subFields[0]);
//                    if (geneUid != null) {
//                        if (csv.getLong(miRnaUid + "." + geneUid) == null) {
//                            // Write mirna-target gene relation
//                            pwMiRnaTargetRel.println(miRnaUid + CsvInfo.SEPARATOR + geneUid + CsvInfo.SEPARATOR + subFields[1]);
//                            csv.putLong(miRnaUid + "." + geneUid, 1);
//                        }
//                    }
//                }
//
//                // Next miRNA
//                rocksIterator.next();
//            }
//        }
//    }

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
}
