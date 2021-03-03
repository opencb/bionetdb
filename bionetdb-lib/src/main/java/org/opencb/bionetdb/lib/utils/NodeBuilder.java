package org.opencb.bionetdb.lib.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v202003jaxb.*;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.core.*;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeBuilder {
    public static final String CHROMOSOME = "chromosome";
    public static final String START = "start";
    public static final String END = "end";
    public static final String REFERENCE = "reference";
    public static final String ALTERNATE = "alternate";
    public static final String STRAND = "strand";
    public static final String TYPE = "type";

    public static final String BIONETDB_PREFIX = "bioNetDB_";

    // These attributes are added by Neo4JQueryParser for CH and DeNovo
    public static final String SAMPLE = BIONETDB_PREFIX + "sample";
    public static final String GENOTYPE = BIONETDB_PREFIX + "genotype";
    public static final String CONSEQUENCE_TYPE = BIONETDB_PREFIX + "consequenceType";
    public static final String TRANSCRIPT = BIONETDB_PREFIX + "transcript";
    public static final String BIOTYPE = BIONETDB_PREFIX + "biotype";

    // These attributes are added by ProteinSystemAnalysis
    public static final String TARGET_PROTEIN = BIONETDB_PREFIX + "targetProtein";
    public static final String COMPLEX = BIONETDB_PREFIX + "complex";
    public static final String REACTION = BIONETDB_PREFIX + "reaction";
    public static final String PANEL_PROTEIN = BIONETDB_PREFIX + "panelProtein";
    public static final String PANEL_GENE = BIONETDB_PREFIX + "panelGene";

    public static Node newNode(long uid, Variant variant) {
        // Since Neo4J can not index long IDs, the allele sequence is removed for the DELETION and INSERTION variant IDs
        String varId = variant.toStringSimple();
        if (variant.getType() == VariantType.DELETION) {
            String[] split = varId.split(":");
            varId = split[0] + ":" + split[1]  + ":DEL:-";
        } else if (variant.getType() == VariantType.INSERTION) {
            String[] split = varId.split(":");
            varId = split[0] + ":" + split[1]  + ":-:INS";
        }

        Node node = new Node(uid, varId, varId, Node.Label.VARIANT);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        if (CollectionUtils.isNotEmpty(variant.getNames())) {
            node.addAttribute("alternativeNames", StringUtils.join(variant.getNames(), ";"));
        }
        node.addAttribute(CHROMOSOME, variant.getChromosome());
        node.addAttribute(START, variant.getStart());
        node.addAttribute(END, variant.getEnd());
        node.addAttribute(REFERENCE, variant.getReference());
        node.addAttribute(ALTERNATE, variant.getAlternate());
        node.addAttribute(STRAND, variant.getStrand());
        node.addAttribute(TYPE, variant.getType().toString());

        if (CollectionUtils.isNotEmpty(variant.getStudies())) {
            // Only one single study is supported
            StudyEntry studyEntry = variant.getStudies().get(0);

            if (CollectionUtils.isNotEmpty(studyEntry.getFiles())) {
                String source = studyEntry.getFiles().get(0).getFileId();
                if (StringUtils.isNotEmpty(source)) {
                    node.addAttribute("source", source);
                }
            }
        }
        return node;
    }

    public static Variant newVariant(Node node) {
        VariantBuilder variantBuilder = Variant.newBuilder();
        ObjectMap attrs = node.getAttributes();
        if (attrs.containsKey(NodeBuilder.CHROMOSOME)) {
            variantBuilder.setChromosome(attrs.getString(NodeBuilder.CHROMOSOME));
        }
        if (attrs.containsKey(NodeBuilder.START)) {
            variantBuilder.setStart(attrs.getInt(NodeBuilder.START));
        }
        if (attrs.containsKey(NodeBuilder.END)) {
            variantBuilder.setEnd(attrs.getInt(NodeBuilder.END));
        }
        if (attrs.containsKey(NodeBuilder.REFERENCE)) {
            variantBuilder.setReference(attrs.getString(NodeBuilder.REFERENCE));
        }
        if (attrs.containsKey(NodeBuilder.ALTERNATE)) {
            variantBuilder.setAlternate(attrs.getString(NodeBuilder.ALTERNATE));
        }
        if (attrs.containsKey(NodeBuilder.TYPE)) {
            variantBuilder.setType(VariantType.valueOf(attrs.getString(NodeBuilder.TYPE)));
        }
        variantBuilder.setStudyId("S");
//        variantBuilder.setFormat("GT");

        return variantBuilder.build();
    }

    public static Node newNode(long uid, StudyEntry studyEntry, Node variantNode) {
        Node node = new Node(uid, variantNode.getId() + "_" + studyEntry.getFiles().get(0).getFileId(), "", Node.Label.VARIANT_FILE_DATA);
        Map<String, String> fileData = studyEntry.getFiles().get(0).getData();
        node.addAttribute("filename", studyEntry.getFiles().get(0).getFileId());
        for (String key : fileData.keySet()) {
            node.addAttribute(key, fileData.get(key));
        }
        return node;
    }

    public static Node newCallNode(long uid, List<String> formatKeys, List<String> formatValues) {
        Node node = new Node(uid, formatValues.get(0), formatValues.get(0), Node.Label.VARIANT_SAMPLE_DATA);
        for (int i = 0; i < formatKeys.size(); i++) {
            node.addAttribute(formatKeys.get(i), formatValues.get(i));
        }
        return node;
    }

    public static Node newNode(long uid, StructuralVariation sv, CsvInfo csvInfo) throws IOException {
        Node node = new Node(uid, null, null, Node.Label.STRUCTURAL_VARIATION);
        node.addAttribute("ciStartLeft", sv.getCiStartLeft());
        node.addAttribute("ciStartRight", sv.getCiStartRight());
        node.addAttribute("ciEndLeft", sv.getCiEndLeft());
        node.addAttribute("ciEndRight", sv.getCiEndRight());
        node.addAttribute("copyNumber", sv.getCopyNumber());
        node.addAttribute("leftSvInSeq", sv.getLeftSvInsSeq());
        node.addAttribute("rightSvInSeq", sv.getRightSvInsSeq());
        node.addAttribute("type", sv.getType());

        if (sv.getBreakend() != null) {
            BufferedWriter bw;

            Node breakendNode = new Node(csvInfo.getAndIncUid(), null, null, Node.Label.BREAKEND);
            if (sv.getBreakend().getOrientation() != null) {
                breakendNode.addAttribute("orientation", sv.getBreakend().getOrientation().name());
            }
            bw = csvInfo.getWriter(Node.Label.BREAKEND.name());
            bw.write(csvInfo.nodeLine(breakendNode));
            bw.newLine();

            bw = csvInfo.getWriter(CsvInfo.RelationFilename.HAS___STRUCTURAL_VARIATION___BREAKEND.name());
            bw.write(csvInfo.relationLine(node.getUid(), breakendNode.getUid()));
            bw.newLine();

            if (sv.getBreakend().getMate() != null) {
                Node mateNode = new Node(csvInfo.getAndIncUid(), null, null, Node.Label.BREAKEND_MATE);
                mateNode.addAttribute("chromosome", sv.getBreakend().getMate().getChromosome());
                mateNode.addAttribute("position", sv.getBreakend().getMate().getPosition().intValue());
                mateNode.addAttribute("ciPositionLeft", sv.getBreakend().getMate().getCiPositionLeft());
                mateNode.addAttribute("ciPositionRight", sv.getBreakend().getMate().getCiPositionRight());

                bw = csvInfo.getWriter(Node.Label.BREAKEND_MATE.name());
                bw.write(csvInfo.nodeLine(mateNode));
                bw.newLine();

                bw = csvInfo.getWriter(CsvInfo.RelationFilename.MATE___BREAKEND___BREAKEND_MATE.name());
                bw.write(csvInfo.relationLine(breakendNode.getUid(), mateNode.getUid()));
                bw.newLine();
            }
        }

        return node;
    }

    public static Node newNode(long uid, PopulationFrequency popFreq) {
        Node node = new Node(uid, popFreq.getPopulation(), popFreq.getPopulation(), Node.Label.VARIANT_POPULATION_FREQUENCY);
        node.addAttribute("study", popFreq.getStudy());
        node.addAttribute("population", popFreq.getPopulation());
        node.addAttribute("refAlleleFreq", popFreq.getRefAlleleFreq());
        node.addAttribute("altAlleleFreq", popFreq.getAltAlleleFreq());
        return node;
    }

    public static Node newNode(long uid, Score score, Node.Label nodeLabel) {
        Node node = new Node(uid, score.getSource(), null, nodeLabel);
        node.addAttribute("score", score.getScore());
        node.addAttribute("source", score.getSource());
        node.addAttribute("description", score.getDescription());
        return node;
    }

    public static Node newNode(long uid, EvidenceEntry evidence, Node.Label nodeLabel, CsvInfo csvInfo) throws IOException {
        Node node = new Node(uid, evidence.getId(), evidence.getId(), nodeLabel);
        node.addAttribute("url", evidence.getUrl());

        if (evidence.getSource() != null) {
            node.addAttribute("sourceName", evidence.getSource().getName());
            node.addAttribute("sourceVersion", evidence.getSource().getVersion());
            node.addAttribute("sourceDate", evidence.getSource().getDate());
        }

        if (CollectionUtils.isNotEmpty(evidence.getAlleleOrigin())) {
            StringBuilder alleleOri = new StringBuilder();
            for (AlleleOrigin alleleOrigin : evidence.getAlleleOrigin()) {
                if (alleleOri.length() > 0 && alleleOrigin.name() != null) {
                    alleleOri.append(",");
                }
                alleleOri.append(alleleOrigin.name());
            }
            node.addAttribute("alleleOrigin", alleleOri.toString());
        }

        BufferedWriter bw;

        if (CollectionUtils.isNotEmpty(evidence.getSubmissions())) {
            for (EvidenceSubmission submission : evidence.getSubmissions()) {
                Node submissionNode = newNode(csvInfo.getAndIncUid(), submission);

                bw = csvInfo.getWriter(Node.Label.EVIDENCE_SUBMISSION.toString());
                bw.write(csvInfo.nodeLine(submissionNode));
                bw.newLine();

                bw = csvInfo.getWriter("HAS___" + nodeLabel.name() + "___EVIDENCE_SUBMISSION");
                bw.write(csvInfo.relationLine(node.getUid(), submissionNode.getUid()));
                bw.newLine();
            }

        }

        if (evidence.getSomaticInformation() != null) {
            node.addAttribute("primarySite", evidence.getSomaticInformation().getPrimarySite());
            node.addAttribute("siteSubtype", evidence.getSomaticInformation().getSiteSubtype());
            node.addAttribute("primaryHistology", evidence.getSomaticInformation().getPrimaryHistology());
            node.addAttribute("histologySubtype", evidence.getSomaticInformation().getHistologySubtype());
            node.addAttribute("tumorOrigin", evidence.getSomaticInformation().getTumourOrigin());
            node.addAttribute("sampleSource", evidence.getSomaticInformation().getSampleSource());
        }

        if (CollectionUtils.isNotEmpty(evidence.getHeritableTraits())) {
            for (HeritableTrait heritableTrait : evidence.getHeritableTraits()) {
                Node heritableNode = newNode(csvInfo.getAndIncUid(), heritableTrait);

                bw = csvInfo.getWriter(Node.Label.HERITABLE_TRAIT.name());
                bw.write(csvInfo.nodeLine(heritableNode));
                bw.newLine();

                bw = csvInfo.getWriter("HAS___" + nodeLabel.name() + "___HERITABLE_TRAIT");
                bw.write(csvInfo.relationLine(node.getUid(), heritableNode.getUid()));
                bw.newLine();
            }
        }

        if (CollectionUtils.isNotEmpty(evidence.getGenomicFeatures())) {
            for (org.opencb.biodata.models.variant.avro.GenomicFeature genomicFeature : evidence.getGenomicFeatures()) {
                Node featureNode = newNode(csvInfo.getAndIncUid(), genomicFeature);

                bw = csvInfo.getWriter(Node.Label.GENOMIC_FEATURE.name());
                bw.write(csvInfo.nodeLine(featureNode));
                bw.newLine();

                bw = csvInfo.getWriter("HAS___" + nodeLabel.name() + "___GENOMIC_FEATURE");
                bw.write(csvInfo.relationLine(node.getUid(), featureNode.getUid()));
                bw.newLine();
            }
        }

        if (evidence.getVariantClassification() != null) {
            Node varClassificationNode = newNode(csvInfo.getAndIncUid(), evidence.getVariantClassification());

            bw = csvInfo.getWriter(Node.Label.VARIANT_CLASSIFICATION.name());
            bw.write(csvInfo.nodeLine(varClassificationNode));
            bw.newLine();

            bw = csvInfo.getWriter("HAS___" + nodeLabel.name() + "___VARIANT_CLASSIFICATION");
            bw.write(csvInfo.relationLine(node.getUid(), varClassificationNode.getUid()));
            bw.newLine();
        }

        node.addAttribute("impact", evidence.getImpact());
        node.addAttribute("confidence", evidence.getConfidence());
        node.addAttribute("consistencyStatus", evidence.getConsistencyStatus());
        node.addAttribute("ethnicity", evidence.getEthnicity());
        node.addAttribute("penetrance", evidence.getPenetrance());
        node.addAttribute("variableExpressivity", evidence.getVariableExpressivity());
        node.addAttribute("description", evidence.getDescription());

        if (CollectionUtils.isNotEmpty(evidence.getAdditionalProperties())) {
            for (Property property : evidence.getAdditionalProperties()) {
                Node propertyNode = newNode(csvInfo.getAndIncUid(), property);

                bw = csvInfo.getWriter(Node.Label.PROPERTY.name());
                bw.write(csvInfo.nodeLine(propertyNode));
                bw.newLine();

                bw = csvInfo.getWriter("HAS___" + nodeLabel.name() + "___PROPERTY");
                bw.write(csvInfo.relationLine(node.getUid(), propertyNode.getUid()));
                bw.newLine();
            }
        }

        if (CollectionUtils.isNotEmpty(evidence.getBibliography())) {
            StringBuilder sb = new StringBuilder();
            for (String biblio : evidence.getBibliography()) {
                if (StringUtils.isNotEmpty(biblio)) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(biblio.replace("\r\n", " ").replace("\r", " ").replace("\n", " "));
                }
            }
            node.addAttribute("bibliography", sb.toString());
        }

        return node;
    }

    public static Node newNode(long uid, EvidenceSubmission evidenceSubmission) {
        Node node = new Node(uid, evidenceSubmission.getId(), evidenceSubmission.getId(), Node.Label.EVIDENCE_SUBMISSION);
        node.addAttribute("submitter", evidenceSubmission.getSubmitter());
        node.addAttribute("date", evidenceSubmission.getDate());
        return node;
    }

    public static Node newNode(long uid, HeritableTrait heritableTrait) {
        Node node = new Node(uid, null, null, Node.Label.HERITABLE_TRAIT);
        node.addAttribute("trait", heritableTrait.getTrait());
        node.addAttribute("inheritanceMode", heritableTrait.getInheritanceMode());
        return node;
    }

    public static Node newNode(long uid,  org.opencb.biodata.models.variant.avro.GenomicFeature genomicFeature) {
        Node node = new Node(uid, null, null, Node.Label.GENOMIC_FEATURE);
        node.addAttribute("featureType", genomicFeature.getFeatureType());
        node.addAttribute("ensemblId", genomicFeature.getEnsemblId());
        return node;
    }

    public static Node newNode(long uid,  org.opencb.biodata.models.variant.avro.VariantClassification variantClassification) {
        Node node = new Node(uid, null, null, Node.Label.VARIANT_CLASSIFICATION);
        node.addAttribute("clinicalSignificance", variantClassification.getClinicalSignificance());
        node.addAttribute("drugResponseClassification", variantClassification.getDrugResponseClassification());
        node.addAttribute("traitAssociation", variantClassification.getTraitAssociation());
        node.addAttribute("tumorigenesisClassification", variantClassification.getTumorigenesisClassification());
        node.addAttribute("functionalEffect", variantClassification.getDrugResponseClassification());
        return node;
    }

    public static Node newNode(long uid, Repeat repeat) {
        Node node = new Node(uid, repeat.getId(), null, Node.Label.REPEAT);
        node.addAttribute("chromosome", repeat.getChromosome());
        node.addAttribute("start", repeat.getStart());
        node.addAttribute("end", repeat.getEnd());
        node.addAttribute("period", repeat.getPeriod());
        node.addAttribute("consensusSize", repeat.getConsensusSize());
        node.addAttribute("copyNumber", repeat.getCopyNumber());
        node.addAttribute("percentageMatch", repeat.getPercentageMatch());
        node.addAttribute("score", repeat.getScore());
        node.addAttribute("source", repeat.getSource());
        return node;
    }

    public static Node newNode(long uid, Cytoband cytoband) {
        Node node = new Node(uid, null, cytoband.getName(), Node.Label.CYTOBAND);
        node.addAttribute("chromosome", cytoband.getChromosome());
        node.addAttribute("start", cytoband.getStart());
        node.addAttribute("end", cytoband.getEnd());
        node.addAttribute("stain", cytoband.getStain());
        return node;
    }
    public static Node newNode(long uid, Drug drug) {
        Node node = new Node(uid, null, null, Node.Label.VARIANT_DRUG_INTERACTION);
        node.addAttribute("therapeuticContext", drug.getTherapeuticContext());
        node.addAttribute("pathway", drug.getPathway());
        node.addAttribute("effect", drug.getEffect());
        node.addAttribute("association", drug.getAssociation());
        node.addAttribute("status", drug.getStatus());
        node.addAttribute("evidence", drug.getEvidence());
        if (CollectionUtils.isNotEmpty(drug.getBibliography())) {
            StringBuilder sb = new StringBuilder();
            for (String biblio : drug.getBibliography()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(biblio.replace("\r\n", " ").replace("\r", " ").replace("\n", " "));
            }
            node.addAttribute("bibliography", sb.toString());
        }
        return node;
    }


    public static Node newNode(long uid, ConsequenceType ct) {
        Node node = new Node(uid, ct.getBiotype(), null, Node.Label.VARIANT_CONSEQUENCE_TYPE);
        node.addAttribute("biotype", ct.getBiotype());
        node.addAttribute("cdnaPosition", ct.getCdnaPosition());
        node.addAttribute("cdsPosition", ct.getCdsPosition());
        node.addAttribute("codon", ct.getCodon());
        node.addAttribute("strand", ct.getStrand());
        node.addAttribute("gene", ct.getEnsemblGeneId());
        node.addAttribute("transcript", ct.getEnsemblTranscriptId());
        // Transcript annotation flags
        if (CollectionUtils.isNotEmpty(ct.getTranscriptAnnotationFlags())) {
            node.addAttribute("transcriptAnnotationFlags", StringUtils.join(ct.getTranscriptAnnotationFlags(), ","));
        }
        // Exon overlap
        if (CollectionUtils.isNotEmpty(ct.getExonOverlap())) {
            StringBuilder overlaps = new StringBuilder();
            overlaps.append(ct.getExonOverlap().get(0).getNumber()).append(":").append(ct.getExonOverlap().get(0).getPercentage());
            for (int i = 1; i < ct.getExonOverlap().size(); i++) {
                overlaps.append(",");
                overlaps.append(ct.getExonOverlap().get(i).getNumber()).append(":").append(ct.getExonOverlap().get(i).getPercentage());
            }
            node.addAttribute("exonOverlap", overlaps.toString());
        }
        return node;
    }

    public static Node newNode(long uid,  Property property) {
        Node node = new Node(uid, property.getId(), property.getName(), Node.Label.PROPERTY);
        node.addAttribute("value", property.getValue());
        return node;
    }


    public static Node newNode(long uid, ProteinVariantAnnotation annotation) {
        Node node = new Node(uid, annotation.getUniprotAccession(), annotation.getUniprotName(),
                Node.Label.PROTEIN_VARIANT_ANNOTATION);
        node.addAttribute("position", annotation.getPosition());
        node.addAttribute("reference", annotation.getReference());
        node.addAttribute("alternate", annotation.getAlternate());
        node.addAttribute("functionalDescription", annotation.getFunctionalDescription());
        return node;
    }

    public static Node newNode(long uid, ProteinFeature feature) {
        Node node = new Node(uid, null, feature.getId(), Node.Label.PROTEIN_FEATURE);
        node.addAttribute("start", feature.getStart());
        node.addAttribute("end", feature.getEnd());
        node.addAttribute("type", feature.getType());
        node.addAttribute("description", feature.getDescription());
        return node;
    }

    public static Node newNode(long uid, Gene gene) {
        Node node = new Node(uid, gene.getId(), gene.getName(), Node.Label.GENE);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        switch (gene.getSource()) {
            case "ensembl":
                node.getLabels().add(Node.Label.ENSEMBL_GENE);
                break;
            case "refseq":
                node.getLabels().add(Node.Label.REFSEQ_GENE);
                break;
            default:
                break;
        }
        node.addAttribute("biotype", gene.getBiotype());
        node.addAttribute("chromosome", gene.getChromosome());
        node.addAttribute("start", gene.getStart());
        node.addAttribute("end", gene.getEnd());
        node.addAttribute("strand", gene.getStrand());
        node.addAttribute("description", gene.getDescription());
        node.addAttribute("version", gene.getVersion());
        node.addAttribute("source", gene.getSource());
        node.addAttribute("status", gene.getStatus());
        return node;
    }

    public static Node newNode(long uid, MiRnaGene miRna) {
        Node node = new Node(uid, miRna.getId(), miRna.getId(), Node.Label.MIRNA);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        node.addAttribute("accession", miRna.getAccession());
        node.addAttribute("status", miRna.getStatus());
        node.addAttribute("sequence", miRna.getSequence());
        return node;
    }

    public static Node newNode(long uid, MiRnaMature miRnaMature) {
        Node node = new Node(uid, miRnaMature.getId(), miRnaMature.getId(), Node.Label.MIRNA_MATURE);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        node.addAttribute("accession", miRnaMature.getAccession());
        node.addAttribute("sequence", miRnaMature.getSequence());
        node.addAttribute("start", miRnaMature.getStart());
        node.addAttribute("end", miRnaMature.getEnd());
        return node;
    }

    public static Node newNode(long uid, GeneDrugInteraction drug) {
        Node node = new Node(uid, null, null, Node.Label.GENE_DRUG_INTERACTION);
        node.addAttribute("source", drug.getSource());
        node.addAttribute("type", drug.getType());
        node.addAttribute("studyType", drug.getStudyType());
        node.addAttribute("interactionType", drug.getInteractionType());
        node.addAttribute("chemblId", drug.getChemblId());
        if (CollectionUtils.isNotEmpty(drug.getPublications())) {
            node.addAttribute("publications", StringUtils.join(drug.getPublications(), ","));
        }
        return node;
    }

    public static Node newNode(long uid,  Expression expression) {
        Node node = new Node(uid, null, null, Node.Label.GENE_EXPRESSION);
        node.addAttribute("transcriptId", expression.getTranscriptId());
        node.addAttribute("expression", expression.getExpression());
        node.addAttribute("experimentId", expression.getExperimentId());
        node.addAttribute("technologyPlatform", expression.getTechnologyPlatform());
        node.addAttribute("factorValue", expression.getFactorValue());
        node.addAttribute("pValue", expression.getPvalue());
        node.addAttribute("experimentalFactor", expression.getExperimentalFactor());
        return node;
    }

    public static Node newNode(long uid, GeneTraitAssociation disease) {
        Node node = new Node(uid, disease.getId(), disease.getName(), Node.Label.GENE_TRAIT_ASSOCIATION);
        node.addAttribute("hpo", disease.getHpo());
        node.addAttribute("numberOfPubmeds", disease.getNumberOfPubmeds());
        node.addAttribute("score", disease.getScore());
        node.addAttribute("source", disease.getSource());
        if (CollectionUtils.isNotEmpty(disease.getSources())) {
            node.addAttribute("sources", StringUtils.join(disease.getSources(), ","));
        }
        if (CollectionUtils.isNotEmpty(disease.getAssociationTypes())) {
            node.addAttribute("associationTypes", StringUtils.join(disease.getAssociationTypes(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, Constraint constraint) {
        Node node = new Node(uid, null, constraint.getName(), Node.Label.TRANSCRIPT_CONSTRAINT_SCORE);
        node.addAttribute("source", constraint.getSource());
        node.addAttribute("method", constraint.getMethod());
        node.addAttribute("value", constraint.getValue());
        return node;
    }


    public static Node newNode(long uid, Transcript transcript) {
        Node node = new Node(uid, transcript.getId(), transcript.getName(), Node.Label.TRANSCRIPT);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        switch (transcript.getSource()) {
            case "ensembl":
                node.getLabels().add(Node.Label.ENSEMBL_TRANSCRIPT);
                break;
            case "refseq":
                node.getLabels().add(Node.Label.REFSEQ_TRANSCRIPT);
                break;
            default:
                break;
        }
        node.addAttribute("chromosome", transcript.getChromosome());
        node.addAttribute("start", transcript.getStart());
        node.addAttribute("end", transcript.getEnd());
        node.addAttribute("strand", transcript.getStrand());
        node.addAttribute("biotype", transcript.getBiotype());
        node.addAttribute("status", transcript.getStatus());
        node.addAttribute("genomicCodingStart", transcript.getGenomicCodingStart());
        node.addAttribute("genomicCodingEnd", transcript.getGenomicCodingEnd());
        node.addAttribute("cdnaCodingStart", transcript.getCdnaCodingStart());
        node.addAttribute("cdnaCodingEnd", transcript.getCdnaCodingEnd());
        node.addAttribute("cdsLength", transcript.getCdsLength());
        node.addAttribute("description", transcript.getDescription());
        node.addAttribute("version", transcript.getVersion());
        node.addAttribute("source", transcript.getSource());
        if (CollectionUtils.isNotEmpty(transcript.getFlags())) {
            node.addAttribute("annotationFlags", StringUtils.join(transcript.getFlags(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, Exon exon, String source) {
        Node node = new Node(uid, exon.getId(), null, Node.Label.EXON);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        switch (source) {
            case "ensembl":
                node.getLabels().add(Node.Label.ENSEMBL_EXON);
                break;
            case "refseq":
                node.getLabels().add(Node.Label.REFSEQ_EXON);
                break;
            default:
                break;
        }
        node.addAttribute("chromosome", exon.getChromosome());
        node.addAttribute("start", exon.getStart());
        node.addAttribute("end", exon.getEnd());
        node.addAttribute("strand", exon.getStrand());
        node.addAttribute("genomicCodingStart", exon.getGenomicCodingStart());
        node.addAttribute("genomicCodingEnd", exon.getGenomicCodingEnd());
        node.addAttribute("cdnaCodingStart", exon.getCdnaCodingStart());
        node.addAttribute("cdnaCodingEnd", exon.getCdnaCodingEnd());
        node.addAttribute("cdsStart", exon.getCdsStart());
        node.addAttribute("cdsEnd", exon.getCdsEnd());
        node.addAttribute("phase", exon.getPhase());
        node.addAttribute("exonNumber", exon.getExonNumber());
        node.addAttribute("source", source);
        return node;
    }

    public static Node newNode(long uid, TranscriptTfbs tfbs) {
        Node node = new Node(uid, tfbs.getId(), null, Node.Label.TFBS);
        node.getLabels().add(Node.Label.DNA);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        node.addAttribute("pfmId", tfbs.getPfmId());
        node.addAttribute("chromosome", tfbs.getChromosome());
        node.addAttribute("start", tfbs.getStart());
        node.addAttribute("end", tfbs.getEnd());
        node.addAttribute("strand", tfbs.getStrand());
        node.addAttribute("type", tfbs.getType());
        node.addAttribute("regulatoryId", tfbs.getRegulatoryId());
        if (CollectionUtils.isNotEmpty(tfbs.getTranscriptionFactors())) {
            node.addAttribute("transcriptionFactors", StringUtils.join(tfbs.getTranscriptionFactors(), ","));
        }
        node.addAttribute("relativeStart", tfbs.getRelativeStart());
        node.addAttribute("relativeEnd", tfbs.getRelativeEnd());
        node.addAttribute("score", tfbs.getScore());
        return node;
    }

    public static Node newNode(long uid, FeatureOntologyTermAnnotation featureOntologyTermAnnotation) {
        Node node = new Node(uid, featureOntologyTermAnnotation.getId(), featureOntologyTermAnnotation.getName(),
                Node.Label.FEATURE_ONTOLOGY_TERM_ANNOTATION);
        node.addAttribute("source", featureOntologyTermAnnotation.getSource());
        if (MapUtils.isNotEmpty(featureOntologyTermAnnotation.getAttributes())) {
            for (String key : featureOntologyTermAnnotation.getAttributes().keySet()) {
                node.addAttribute("attributes_" + key, featureOntologyTermAnnotation.getAttributes().get(key));
            }
        }
        return node;
    }

    public static Node newNode(long uid, AnnotationEvidence annotationEvidence) {
        Node node = new Node(uid, annotationEvidence.getCode(), null, Node.Label.TRANSCRIPT_ANNOTATION_EVIDENCE);
        if (CollectionUtils.isNotEmpty(annotationEvidence.getReferences())) {
            node.addAttribute("references", StringUtils.join(annotationEvidence.getReferences(), ","));
        }
        node.addAttribute("qualifier", annotationEvidence.getQualifier());
        return node;
    }

    public static Node newNode(long uid, Xref xref) {
        Node node = new Node(uid, xref.getId(), null, Node.Label.XREF);
        node.addAttribute("dbName", xref.getDbName());
        node.addAttribute("dbDisplayName", xref.getDbDisplayName());
        node.addAttribute("description", xref.getDescription());
        return node;
    }

    public static Node newNode(long uid, DbReferenceType xref) {
        Node node = new Node(uid, xref.getId(), null, Node.Label.XREF);
        node.addAttribute("dbName", xref.getType());
        return node;
    }

    public static Node newNode(long uid, Entry protein) {
        String id = (CollectionUtils.isNotEmpty(protein.getAccession()) ? protein.getAccession().get(0) : null);
        String name = (CollectionUtils.isNotEmpty(protein.getName()) ? protein.getName().get(0) : null);
        Node node = new Node(uid, id, name, Node.Label.PROTEIN);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        if (CollectionUtils.isNotEmpty(protein.getAccession())) {
            node.addAttribute("accession", StringUtils.join(protein.getAccession(), ","));
        }

        node.addAttribute("dataset", protein.getDataset());
//        node.addAttribute("dbReference", protein.getDbReference());
        if (protein.getProteinExistence() != null) {
            node.addAttribute("proteinExistence", protein.getProteinExistence().getType());
        }
        if (CollectionUtils.isNotEmpty(protein.getEvidence())) {
            StringBuilder sb = new StringBuilder();
            for (EvidenceType evidenceType : protein.getEvidence()) {
                sb.append(evidenceType.getKey()).append(";");
            }
            node.addAttribute("evidence", sb.toString());
        }
//        // Gene location
//        if (protein.getGeneLocation() != null) {
//            for (GeneLocationType location: protein.getGeneLocation()) {
//            }
//        }
//        // Gene type
//        if (CollectionUtils.isNotEmpty(protein.getEnsemblGene())) {
//            protein.getEnsemblGene().get(0).getName().get(0).
//        }

        return node;
    }

    public static Node newNode(long uid, KeywordType keyword) {
        Node node = new Node(uid, keyword.getId(), keyword.getValue(), Node.Label.PROTEIN_KEYWORD);
        if (CollectionUtils.isNotEmpty(keyword.getEvidence())) {
            node.addAttribute("evidence", StringUtils.join(keyword.getEvidence(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, FeatureType feature) {
        Node node = new Node(uid, feature.getId(), feature.getId(), Node.Label.PROTEIN_FEATURE);
        node.addAttribute("type", feature.getType());
        if (CollectionUtils.isNotEmpty(feature.getEvidence())) {
            node.addAttribute("evidence", StringUtils.join(feature.getEvidence(), ","));
        }
        if (feature.getLocation() != null) {
            if (feature.getLocation().getPosition() != null) {
                node.addAttribute("locationPosition", feature.getLocation().getPosition().getPosition());
            }
            if (feature.getLocation().getBegin() != null) {
                node.addAttribute("locationBegin", feature.getLocation().getBegin().getPosition());
            }
            if (feature.getLocation().getEnd() != null) {
                node.addAttribute("locationEnd", feature.getLocation().getEnd().getPosition());
            }
        }
        node.addAttribute("description", feature.getDescription());
        return node;
    }

    public static Node newNode(long uid, DiseasePanel panel) {
        // IMPORTANT: phenotypes, variant, genes, STRs, regions must be created by the caller of this function!

        Node node = new Node(uid, panel.getId(), panel.getName(), Node.Label.DISEASE_PANEL);
        if (CollectionUtils.isNotEmpty(panel.getCategories())) {
            node.addAttribute("categories", panel.getCategories().stream().map(DiseasePanel.PanelCategory::getName)
                    .collect(Collectors.joining(",")));
        }
        if (CollectionUtils.isNotEmpty(panel.getTags())) {
            node.addAttribute("tags", StringUtils.join(panel.getTags(), ","));
        }
        if (MapUtils.isNotEmpty(panel.getStats())) {
            for (String key : panel.getStats().keySet()) {
                node.addAttribute("stats_" + key, String.valueOf(panel.getStats().get(key)));
            }
        }
        if (panel.getSource() != null) {
            node.addAttribute("sourceId", panel.getSource().getId());
            node.addAttribute("sourceName", panel.getSource().getId());
            node.addAttribute("sourceAuthor", panel.getSource().getAuthor());
            node.addAttribute("sourceProject", panel.getSource().getProject());
            node.addAttribute("sourceVersion", panel.getSource().getVersion());
        }
        node.addAttribute("creationDate", panel.getCreationDate());
        node.addAttribute("modificationDate", panel.getModificationDate());
        node.addAttribute("description", panel.getDescription());
        if (MapUtils.isNotEmpty(panel.getAttributes())) {
            for (String key : panel.getAttributes().keySet()) {
                node.addAttribute("attributes_" + key, panel.getAttributes().get(key).toString());
            }
        }
        return node;
    }

    public static Node newNode(long uid, GenomicFeature genomicFeature) {
        Node node = new Node(uid, genomicFeature.getId(), genomicFeature.getId(), Node.Label.GENOMIC_FEATURE);
        node.getLabels().add(Node.Label.DNA);
        node.getLabels().add(Node.Label.PHYSICAL_ENTITY);
        node.addAttribute("type", genomicFeature.getType());
        node.addAttribute("geneName", genomicFeature.getGeneName());
        node.addAttribute("transcriptId", genomicFeature.getTranscriptId());
        // TODO: xrefs
        return node;
    }

    public static Node newNode(long uid, Phenotype phenotype) {
        // IMPORTANT: ontology term node and relation must be created by the caller!

        Node node = new Node(uid, phenotype.getId(), phenotype.getName(), Node.Label.PHENOTYPE);
        node.addAttribute("ageOfOnset", phenotype.getAgeOfOnset());
        if (phenotype.getStatus() != null) {
            node.addAttribute("status", phenotype.getStatus().name());
        }
        return node;
    }

    public static Node newNode(long uid, OntologyTermAnnotation ontologyTerm) {
        Node node = new Node(uid, ontologyTerm.getId(), ontologyTerm.getName(), Node.Label.ONTOLOGY_TERM);
        node.addAttribute("source", ontologyTerm.getSource());
        if (MapUtils.isNotEmpty(ontologyTerm.getAttributes())) {
            for (String key : ontologyTerm.getAttributes().keySet()) {
                node.addAttribute("attributes_" + key, ontologyTerm.getAttributes().get(key));
            }
        }
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.GenePanel panelGene) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelGene.getId(), panelGene.getName(), Node.Label.PANEL_GENE);
        addDiseasePanelCommon(panelGene, node);
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.VariantPanel panelVariant) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelVariant.getId(), panelVariant.getId(), Node.Label.PANEL_VARIANT);
        node.addAttribute("alternate", panelVariant.getAlternate());
        node.addAttribute("reference", panelVariant.getReference());
        addDiseasePanelCommon(panelVariant, node);
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.STR panelStr) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelStr.getId(), panelStr.getId(), Node.Label.PANEL_STR);
        node.addAttribute("repeatedSequence", panelStr.getRepeatedSequence());
        node.addAttribute("normalRepeats", String.valueOf(panelStr.getNormalRepeats()));
        node.addAttribute("pathogenicRepeats", String.valueOf(panelStr.getPathogenicRepeats()));
        addDiseasePanelCommon(panelStr, node);
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.RegionPanel panelRegion) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelRegion.getId(), panelRegion.getId(), Node.Label.PANEL_REGION);
        node.addAttribute("description", panelRegion.getDescription());
        if (panelRegion.getTypeOfVariants() != null) {
            node.addAttribute("typeOfVariants", panelRegion.getTypeOfVariants().name());
        }
        node.addAttribute("haploinsufficiencyScore", panelRegion.getHaploinsufficiencyScore());
        node.addAttribute("triplosensitivityScore", panelRegion.getTriplosensitivityScore());
        node.addAttribute("requiredOverlapPercentage", String.valueOf(panelRegion.getRequiredOverlapPercentage()));
        addDiseasePanelCommon(panelRegion, node);
        return node;
    }

    public static Node newNode(long uid, Individual individual) {
        Node node = new Node(uid, individual.getId(), individual.getId(), Node.Label.INDIVIDUAL);
        node.addAttribute("sex", individual.getSex());
        // TODO: make a relation with the phenotype node ???
        node.addAttribute("phenotype", individual.getPhenotype());
        return node;
    }


//    public static Node newNode(long uid, File file) {
//        // IMPORTANT: software, experiment and sample nodes and relations must be created by the caller!
//
//        Node node = new Node(uid, file.getId(), file.getName(), Node.Label.VARIANT_FILE);
//        node.addAttribute("uuid", file.getUuid());
//        if (file.getLabels() != null) {
//            node.addAttribute("type", file.getLabels().name());
//        }
//        if (file.getFormat() != null) {
//            node.addAttribute("format", file.getFormat().name());
//        }
//        if (file.getBioformat() != null) {
//            node.addAttribute("bioformat", file.getBioformat().name());
//        }
//        node.addAttribute("checksum", file.getChecksum());
//        if (file.getUri() != null) {
//            node.addAttribute("uri", file.getUri().toString());
//        }
//        node.addAttribute("path", file.getPath());
//        node.addAttribute("release", String.valueOf(file.getRelease()));
//        node.addAttribute("creationDate", String.valueOf(file.getCreationDate()));
//        node.addAttribute("modificationDate", String.valueOf(file.getModificationDate()));
//        node.addAttribute("description", String.valueOf(file.getDescription()));
//        addStatus(file.getStatus(), node);
//        node.addAttribute("external", file.isExternal());
//        node.addAttribute("size", String.valueOf(file.getSize()));
//        if (CollectionUtils.isNotEmpty(file.getTags())) {
//            node.addAttribute("tags", StringUtils.join(file.getTags(), ","));
//        }
//        // TODO: file.getRelatedFiles(), File.RelatedFile
//        // TODO: file.getIndex(), FileIndex
//        addObjectMap(file.getStats(), node, "stats_");
//        addObjectMap(file.getAttributes(), node);
//        return node;
//    }

//    public static Node newNode(long uid, FileExperiment experiment) {
//        Node node = new Node(uid, "", "", Node.Label.EXPERIMENT);
//        node.addAttribute("library", experiment.getLibrary());
//        node.addAttribute("platform", experiment.getPlatform());
//        node.addAttribute("manufacturer", experiment.getManufacturer());
//        node.addAttribute("date", experiment.getDate());
//        node.addAttribute("lab", experiment.getLab());
//        node.addAttribute("center", experiment.getCenter());
//        node.addAttribute("responsible", experiment.getResponsible());
//        node.addAttribute("description", experiment.getDescription());
//        addObjectMap(experiment.getAttributes(), node);
//        return node;
//    }

    public static String getSoftwareId(Software software) {
        StringBuilder id = new StringBuilder();
        id.append(software.getName() != null ? software.getName() : "").append("-");
        id.append(software.getVersion() != null ? software.getVersion() : "").append("-");
        id.append(software.getCommit() != null ? software.getCommit() : "");
        return id.toString();
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

//    private static void addAttributes(Map<String, Object> attributes, Node node) {
//        if (MapUtils.isNotEmpty(attributes)) {
//            for (String key : attributes.keySet()) {
//                node.addAttribute("attributes_" + key, attributes.get(key).toString());
//            }
//        }
//    }

    private static void addObjectMap(Map<String, Object> attributes, Node node) {
        addObjectMap(attributes, node, "attributes_");
    }

    private static void addObjectMap(Map<String, Object> attributes, Node node, String prefix) {
        if (MapUtils.isNotEmpty(attributes)) {
            for (String key : attributes.keySet()) {
                node.addAttribute(prefix + key, attributes.get(key).toString());
            }
        }
    }

    private static void addMap(Map<String, String> attributes, Node node) {
        addMap(attributes, node, "attributes_");
    }

    private static void addMap(Map<String, String> attributes, Node node, String prefix) {
        if (MapUtils.isNotEmpty(attributes)) {
            for (String key : attributes.keySet()) {
                node.addAttribute(prefix + key, attributes.get(key));
            }
        }
    }

//    private static void addStatus(CustomStatus status, Node node) {
//        if (status != null) {
//            node.addAttribute("status_name", status.getName());
//            node.addAttribute("status_description", status.getDescription());
//            node.addAttribute("status_date", status.getDate());
//        }
//    }

    private static void addDiseasePanelCommon(DiseasePanel.Common common, Node node) {
        node.addAttribute("modeOfInheritance", common.getModeOfInheritance());
        if (common.getPenetrance() != null) {
            node.addAttribute("penetrance", common.getPenetrance().name());
        }
        node.addAttribute("confidence", common.getConfidence());
        if (CollectionUtils.isNotEmpty(common.getEvidences())) {
            node.addAttribute("evidences", StringUtils.join(common.getEvidences(), ","));
        }
        if (CollectionUtils.isNotEmpty(common.getPublications())) {
            String pubs = StringUtils.join(common.getPublications(), ",").replace("\r\n", " ").replace("\r", " ").replace("\n", " ");
            node.addAttribute("publications", pubs);
        }
        if (CollectionUtils.isNotEmpty(common.getCoordinates())) {
            node.addAttribute("coordinates", common.getCoordinates().stream().map(c -> c.getAssembly() + "+" + c.getLocation() + "+"
                    + c.getSource()).collect(Collectors.joining(",")));
        }
    }
}
