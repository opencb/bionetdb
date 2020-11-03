package org.opencb.bionetdb.lib.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v202003jaxb.*;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.opencb.biodata.models.core.*;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.ListUtils;

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
        Node node = new Node(uid, variant.toStringSimple(), variant.getId(), Node.Type.VARIANT);
        if (ListUtils.isNotEmpty(variant.getNames())) {
            node.addAttribute("alternativeNames", StringUtils.join(variant.getNames(), ";"));
        }
        node.addAttribute(CHROMOSOME, variant.getChromosome());
        node.addAttribute(START, variant.getStart());
        node.addAttribute(END, variant.getEnd());
        node.addAttribute(REFERENCE, variant.getReference());
        node.addAttribute(ALTERNATE, variant.getAlternate());
        node.addAttribute(STRAND, variant.getStrand());
        node.addAttribute(TYPE, variant.getType().toString());

        if (ListUtils.isNotEmpty(variant.getStudies())) {
            // Only one single study is supported
            StudyEntry studyEntry = variant.getStudies().get(0);

            if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                String source = studyEntry.getFiles().get(0).getFileId();
                if (StringUtils.isNotEmpty(source)) {
                    node.setSource(source);
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
        Node node = new Node(uid, variantNode.getId() + "_" + variantNode.getSource(), "", Node.Type.VARIANT_FILE_INFO);
        Map<String, String> fileData = studyEntry.getFiles().get(0).getData();
        node.addAttribute("filename", studyEntry.getFiles().get(0).getFileId());
        for (String key : fileData.keySet()) {
            node.addAttribute(key, fileData.get(key));
        }
        return node;
    }

    public static Node newCallNode(long uid, List<String> formatKeys, List<String> formatValues) {
        Node node = new Node(uid, formatValues.get(0), formatValues.get(0), Node.Type.VARIANT_CALL);
        for (int i = 0; i < formatKeys.size(); i++) {
            node.addAttribute(formatKeys.get(i), formatValues.get(i));
        }
        return node;
    }

    public static Node newNode(long uid, PopulationFrequency popFreq) {
        Node node = new Node(uid, popFreq.getPopulation(), popFreq.getPopulation(), Node.Type.POPULATION_FREQUENCY);
        node.addAttribute("study", popFreq.getStudy());
        node.addAttribute("population", popFreq.getPopulation());
        node.addAttribute("refAlleleFreq", popFreq.getRefAlleleFreq());
        node.addAttribute("altAlleleFreq", popFreq.getAltAlleleFreq());
        return node;
    }

    public static Node newNode(long uid, Score score, Node.Type nodeType) {
        Node node = new Node(uid, score.getSource(), null, nodeType);
        node.addAttribute("score", score.getScore());
        node.addAttribute("source", score.getSource());
        node.addAttribute("description", score.getDescription());
        return node;
    }

    public static Node newNode(long uid, EvidenceEntry evidence, Node.Type nodeType) {
        Node node = new Node(uid, evidence.getId(), null, nodeType);
        node.addAttribute("url", evidence.getUrl());
        if (ListUtils.isNotEmpty(evidence.getHeritableTraits())) {
            StringBuilder her = new StringBuilder();
            for (HeritableTrait heritableTrait : evidence.getHeritableTraits()) {
                if (her.length() > 0) {
                    her.append(",");
                }
                her.append(heritableTrait.getTrait());
            }
            node.addAttribute("heritableTraits", her.toString());
        }
        if (evidence.getVariantClassification() != null
                && evidence.getVariantClassification().getClinicalSignificance() != null) {
            node.addAttribute("clinicalSignificance", evidence.getVariantClassification().getClinicalSignificance()
                    .name());
        }
        if (evidence.getSource() != null && evidence.getSource().getName() != null) {
            node.addAttribute("source", evidence.getSource().getName());
        }
        if (ListUtils.isNotEmpty(evidence.getAlleleOrigin())) {
            StringBuilder alleleOri = new StringBuilder();
            for (AlleleOrigin alleleOrigin : evidence.getAlleleOrigin()) {
                if (alleleOri.length() > 0 && alleleOrigin.name() != null) {
                    alleleOri.append(",");
                }
                alleleOri.append(alleleOrigin.name());
            }
            node.addAttribute("alleleOrigin", alleleOri.toString());
        }
        return node;
    }

    public static Node newNode(long uid, ConsequenceType ct) {
        Node node = new Node(uid, ct.getBiotype(), null, Node.Type.CONSEQUENCE_TYPE);
        node.addAttribute("biotype", ct.getBiotype());
        node.addAttribute("cdnaPosition", ct.getCdnaPosition());
        node.addAttribute("cdsPosition", ct.getCdsPosition());
        node.addAttribute("codon", ct.getCodon());
        node.addAttribute("strand", ct.getStrand());
        node.addAttribute("gene", ct.getEnsemblGeneId());
        node.addAttribute("transcript", ct.getEnsemblTranscriptId());
        // Transcript annotation flags
        if (ListUtils.isNotEmpty(ct.getTranscriptAnnotationFlags())) {
            node.addAttribute("transcriptAnnotationFlags", StringUtils.join(ct.getTranscriptAnnotationFlags(), ","));
        }
        // Exon overlap
        if (ListUtils.isNotEmpty(ct.getExonOverlap())) {
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

    public static Node newNode(long uid, ProteinVariantAnnotation annotation) {
        Node node = new Node(uid, annotation.getUniprotAccession(), annotation.getUniprotName(),
                Node.Type.PROTEIN_VARIANT_ANNOTATION);
        node.addAttribute("position", annotation.getPosition());
        node.addAttribute("reference", annotation.getReference());
        node.addAttribute("alternate", annotation.getAlternate());
        node.addAttribute("functionalDescription", annotation.getFunctionalDescription());
        return node;
    }

    public static Node newNode(long uid, ProteinFeature feature) {
        Node node = new Node(uid, null, feature.getId(), Node.Type.PROTEIN_FEATURE);
        node.addAttribute("start", feature.getStart());
        node.addAttribute("end", feature.getEnd());
        node.addAttribute("type", feature.getType());
        node.addAttribute("description", feature.getDescription());
        return node;
    }

    public static Node newNode(long uid, Gene gene) {
        Node node = new Node(uid, gene.getId(), gene.getName(), Node.Type.GENE);
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
        Node node = new Node(uid, miRna.getId(), miRna.getId(), Node.Type.MIRNA);
        node.addAttribute("accession", miRna.getAccession());
        node.addAttribute("status", miRna.getStatus());
        node.addAttribute("sequence", miRna.getSequence());
        return node;
    }

    public static Node newNode(long uid, MiRnaMature miRnaMature) {
        Node node = new Node(uid, miRnaMature.getId(), miRnaMature.getId(), Node.Type.MIRNA_MATURE);
        node.addAttribute("accession", miRnaMature.getAccession());
        node.addAttribute("sequence", miRnaMature.getSequence());
        node.addAttribute("start", miRnaMature.getStart());
        node.addAttribute("end", miRnaMature.getEnd());
        return node;
    }

    public static Node newNode(long uid, GeneDrugInteraction drug) {
        Node node = new Node(uid, null, drug.getDrugName(), Node.Type.DRUG);
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

    public static Node newNode(long uid, GeneTraitAssociation disease) {
        Node node = new Node(uid, disease.getId(), disease.getName(), Node.Type.DISEASE);
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
        Node node = new Node(uid, null, constraint.getName(), Node.Type.CONSTRAINT);
        node.addAttribute("source", constraint.getSource());
        node.addAttribute("method", constraint.getMethod());
        node.addAttribute("value", constraint.getValue());
        return node;
    }


    public static Node newNode(long uid, Transcript transcript) {
        Node node = new Node(uid, transcript.getId(), transcript.getName(), Node.Type.TRANSCRIPT);
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
        node.addAttribute("cDnaSequence", transcript.getcDnaSequence());
        // TODO: maybe to remove?
        node.addAttribute("proteinId", transcript.getProteinId());
        // TODO: maybe to remove?
        node.addAttribute("proteinSequence", transcript.getProteinSequence());
        node.addAttribute("description", transcript.getDescription());
        node.addAttribute("version", transcript.getVersion());
        node.addAttribute("source", transcript.getSource());
        if (CollectionUtils.isNotEmpty(transcript.getFlags())) {
            node.addAttribute("annotationFlags", StringUtils.join(transcript.getFlags(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, Exon exon) {
        Node node = new Node(uid, exon.getId(), null, Node.Type.EXON);
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
        node.addAttribute("sequence", exon.getSequence());
        return node;
    }

    public static Node newNode(long uid, TranscriptTfbs tfbs) {
        Node node = new Node(uid, tfbs.getId(), null, Node.Type.TFBS);
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
                Node.Type.FEATURE_ONTOLOGY_TERM_ANNOTATION);
        node.addAttribute("source", featureOntologyTermAnnotation.getSource());
        if (MapUtils.isNotEmpty(featureOntologyTermAnnotation.getAttributes())) {
            for (String key : featureOntologyTermAnnotation.getAttributes().keySet()) {
                node.addAttribute("attributes_" + key, featureOntologyTermAnnotation.getAttributes().get(key));
            }
        }
        return node;
    }

    public static Node newNode(long uid, AnnotationEvidence annotationEvidence) {
        Node node = new Node(uid, annotationEvidence.getCode(), null, Node.Type.ANNOTATION_EVIDENCE);
        if (CollectionUtils.isNotEmpty(annotationEvidence.getReferences())) {
            node.addAttribute("references", StringUtils.join(annotationEvidence.getReferences(), ","));
        }
        node.addAttribute("qualifier", annotationEvidence.getQualifier());
        return node;
    }

    public static Node newNode(long uid, Xref xref) {
        Node node = new Node(uid, xref.getId(), null, Node.Type.XREF);
        node.addAttribute("dbName", xref.getDbName());
        node.addAttribute("dbDisplayName", xref.getDbDisplayName());
        node.addAttribute("description", xref.getDescription());
        return node;
    }

    public static Node newNode(long uid, DbReferenceType xref) {
        Node node = new Node(uid, xref.getId(), null, Node.Type.XREF);
        node.addAttribute("dbName", xref.getType());
        return node;
    }

    public static Node newNode(long uid, Entry protein) {
        String id = (ListUtils.isNotEmpty(protein.getAccession()) ? protein.getAccession().get(0) : null);
        String name = (ListUtils.isNotEmpty(protein.getName()) ? protein.getName().get(0) : null);
        Node node = new Node(uid, id, name, Node.Type.PROTEIN);
        if (ListUtils.isNotEmpty(protein.getAccession())) {
            node.addAttribute("accession", StringUtils.join(protein.getAccession(), ","));
        }
//        if (ListUtils.isNotEmpty(protein.getAccession())) {
//            node.addAttribute("name", StringUtils.join(protein.getName(), ","));
//        }
        node.addAttribute("dataset", protein.getDataset());
//        node.addAttribute("dbReference", protein.getDbReference());
        if (protein.getProteinExistence() != null) {
            node.addAttribute("proteinExistence", protein.getProteinExistence().getType());
        }
        if (ListUtils.isNotEmpty(protein.getEvidence())) {
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
//        if (ListUtils.isNotEmpty(protein.getGene())) {
//            protein.getGene().get(0).getName().get(0).
//        }

        return node;
    }

    public static Node newNode(long uid, KeywordType keyword) {
        Node node = new Node(uid, keyword.getId(), keyword.getValue(), Node.Type.PROTEIN_KEYWORD);
        if (ListUtils.isNotEmpty(keyword.getEvidence())) {
            node.addAttribute("evidence", StringUtils.join(keyword.getEvidence(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, FeatureType feature) {
        Node node = new Node(uid, feature.getId(), null, Node.Type.PROTEIN_FEATURE);
        if (ListUtils.isNotEmpty(feature.getEvidence())) {
            node.addAttribute("evidence", StringUtils.join(feature.getEvidence(), ","));
        }
        if (feature.getLocation() != null) {
            node.addAttribute("location_position", feature.getLocation().getPosition());
            node.addAttribute("location_begin", feature.getLocation().getBegin());
            node.addAttribute("location_end", feature.getLocation().getEnd());
        }
        node.addAttribute("description", feature.getDescription());
        return node;
    }

    public static Node newNode(long uid, DiseasePanel panel) {
        // IMPORTANT: phenotypes, variant, genes, STRs, regions must be created by the caller of this function!

        Node node = new Node(uid, panel.getId(), panel.getName(), Node.Type.PANEL);
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
            node.addAttribute("source_id", panel.getSource().getId());
            node.addAttribute("source_name", panel.getSource().getId());
            node.addAttribute("source_author", panel.getSource().getAuthor());
            node.addAttribute("source_project", panel.getSource().getProject());
            node.addAttribute("source_version", panel.getSource().getVersion());
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

//    public static Node newNode(long uid, ClinicalAnalysis clinicalAnalysis) {
//        Node node = new Node(uid, clinicalAnalysis.getId(), clinicalAnalysis.getId(), Node.Type.CLINICAL_ANALYSIS);
//        node.addAttribute("uuid", clinicalAnalysis.getUuid());
//        node.addAttribute("description", clinicalAnalysis.getDescription());
//        if (clinicalAnalysis.getType() != null) {
//            node.addAttribute("type", clinicalAnalysis.getType().name());
//        }
//        if (clinicalAnalysis.getPriority() != null) {
//            node.addAttribute("priority", clinicalAnalysis.getPriority().name());
//        }
//        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getFlags())) {
//            node.addAttribute("flags", StringUtils.join(clinicalAnalysis.getFlags(), ";"));
//        }
//        node.addAttribute("creationDate", clinicalAnalysis.getCreationDate());
//        node.addAttribute("modificationDate", clinicalAnalysis.getModificationDate());
//        node.addAttribute("dueDate", clinicalAnalysis.getModificationDate());
//        addStatus(clinicalAnalysis.getStatus(), node);
//        if (clinicalAnalysis.getConsent() != null) {
//            node.addAttribute("consent_primaryFindings", clinicalAnalysis.getConsent().getPrimaryFindings().name());
//            node.addAttribute("consent_secondaryFindings", clinicalAnalysis.getConsent().getSecondaryFindings().name());
//            node.addAttribute("consent_carrierFindings", clinicalAnalysis.getConsent().getCarrierFindings().name());
//            node.addAttribute("consent_researchFindings",  clinicalAnalysis.getConsent().getResearchFindings().name());
//        }
//        node.addAttribute("release", clinicalAnalysis.getRelease());
//        return node;
//    }

    public static Node newNode(long uid, ClinicalComment comment) {
        Node node = new Node(uid, "" + uid, "" + uid, Node.Type.COMMENT);
        node.addAttribute("author", comment.getAuthor());
        if (CollectionUtils.isNotEmpty(comment.getTags())) {
            node.addAttribute("tags", StringUtils.join(comment.getTags(), ","));
        }
        node.addAttribute("message", comment.getMessage());
        node.addAttribute("date", comment.getDate());
        return node;
    }

    public static Node newNode(long uid, ClinicalAnalyst analyst) {
        Node node = new Node(uid, "" + uid, "" + uid, Node.Type.CLINICAL_ANALYST);
        node.addAttribute("assignedBy", analyst.getAssignedBy());
        node.addAttribute("id", analyst.getId());
        node.addAttribute("name", analyst.getName());
        node.addAttribute("email", analyst.getEmail());
        node.addAttribute("date", analyst.getDate());
        return node;
    }

    public static Node newNode(long uid, Interpretation interpretation) {
        Node node = new Node(uid, "" + uid, "" + uid, Node.Type.INTERPRETATION);
        node.addAttribute("uuid", interpretation.getUuid());
        node.addAttribute("description", interpretation.getDescription());
        node.addAttribute("status", interpretation.getStatus());
        node.addAttribute("creationDate", interpretation.getCreationDate());
        node.addAttribute("version", interpretation.getVersion());
        //addObjectMap(interpretation.getFilters(), node, "filters_");
        return node;
    }

    public static Node newNode(long uid, Software software) {
        String id = getSoftwareId(software);
        Node node = new Node(uid, id, software.getName(), Node.Type.SOFTWARE);
        node.addAttribute("version", software.getVersion());
        node.addAttribute("repository", software.getRepository());
        node.addAttribute("commit", software.getCommit());
        node.addAttribute("website", software.getWebsite());
        addMap(software.getParams(), node, "params_");
        return node;
    }

    public static Node newNode(long uid, ClinicalVariant clinicalVariant) {
        Node node = new Node(uid, clinicalVariant.toStringSimple(), clinicalVariant.getId(), Node.Type.REPORTED_VARIANT);
        node.addAttribute("status", clinicalVariant.getStatus().name());
        addObjectMap(clinicalVariant.getAttributes(), node);
        return node;
    }

//    public static Node newNode(long uid, Analyst analyst) {
//        Node node = new Node(uid, analyst.getName(), analyst.getName(), Node.Type.ANALYST);
//        node.addAttribute("company", analyst.getCompany());
//        node.addAttribute("email", analyst.getEmail());
//        return node;
//    }

    public static Node newNode(long uid, ClinicalVariantEvidence clinicalVariantEvidence) {
        Node node = new Node(uid, "" + uid, "" + uid, Node.Type.REPORTED_EVENT);
        if (clinicalVariantEvidence.getModeOfInheritance() != null) {
            node.addAttribute("modeOfInheritance", clinicalVariantEvidence.getModeOfInheritance().name());
        }
        if (clinicalVariantEvidence.getPenetrance() != null) {
            node.addAttribute("penetrance", clinicalVariantEvidence.getPenetrance().name());
        }
        if (CollectionUtils.isNotEmpty(clinicalVariantEvidence.getCompoundHeterozygousVariantIds())) {
            node.addAttribute("compoundHeterozygousVariantIds", StringUtils.join(clinicalVariantEvidence
                    .getCompoundHeterozygousVariantIds(), ","));
        }
        node.addAttribute("score", clinicalVariantEvidence.getScore());
        node.addAttribute("fullyExplainPhenotypes", clinicalVariantEvidence.isFullyExplainPhenotypes());
        if (clinicalVariantEvidence.getRoleInCancer() != null) {
            node.addAttribute("roleInCancer", clinicalVariantEvidence.getRoleInCancer().name());
        }
        node.addAttribute("actionable", clinicalVariantEvidence.isActionable());
        node.addAttribute("justification", clinicalVariantEvidence.getJustification());
        if (clinicalVariantEvidence.getClassification() != null) {
            VariantClassification classification = clinicalVariantEvidence.getClassification();
            // Tier
            if (StringUtils.isNotEmpty(classification.getTier())) {
                node.addAttribute("classification_tier", classification.getTier());
            }
            // ACMG
            if (CollectionUtils.isNotEmpty(classification.getAcmg())) {
                node.addAttribute("classification_acmg", StringUtils.join(classification.getAcmg(), ","));
            }
            // Clinical significance
            if (classification.getClinicalSignificance() != null) {
                node.addAttribute("classification_clinicalSignificance", classification.getClinicalSignificance().name());
            }
            // Drug response
            if (classification.getDrugResponse() != null) {
                node.addAttribute("classification_drugResponse", classification.getDrugResponse().name());
            }
            // Trait association
            if (classification.getTraitAssociation() != null) {
                node.addAttribute("classification_traitAssociation", classification.getTraitAssociation().name());
            }
            // Functional effect
            if (classification.getFunctionalEffect() != null) {
                node.addAttribute("classification_functionalEffect", classification.getFunctionalEffect().name());
            }
            // Tumorigenesis
            if (classification.getTumorigenesis() != null) {
                node.addAttribute("classification_tumorigenesis", classification.getTumorigenesis().name());
            }
            // Other
            if (CollectionUtils.isNotEmpty(classification.getOther())) {
                node.addAttribute("classification_other", StringUtils.join(classification.getOther(), "---"));
            }
        }
        return node;
    }

    public static Node newNode(long uid, GenomicFeature genomicFeature) {
        Node node = new Node(uid, genomicFeature.getId(), genomicFeature.getId(), Node.Type.GENOMIC_FEATURE);
        node.addAttribute("type", genomicFeature.getType());
        node.addAttribute("geneName", genomicFeature.getGeneName());
        node.addAttribute("transcriptId", genomicFeature.getTranscriptId());
        // TODO: xrefs
        return node;
    }

    public static Node newNode(long uid, Phenotype phenotype) {
        // IMPORTANT: ontology term node and relation must be created by the caller!

        Node node = new Node(uid, phenotype.getId(), phenotype.getName(), Node.Type.PHENOTYPE);
        node.addAttribute("ageOfOnset", phenotype.getAgeOfOnset());
        if (phenotype.getStatus() != null) {
            node.addAttribute("status", phenotype.getStatus().name());
        }
        return node;
    }

    public static Node newNode(long uid, OntologyTermAnnotation ontologyTerm) {
        Node node = new Node(uid, ontologyTerm.getId(), ontologyTerm.getName(), Node.Type.ONTOLOGY_TERM);
        node.addAttribute("source", ontologyTerm.getSource());
        if (MapUtils.isNotEmpty(ontologyTerm.getAttributes())) {
            for (String key : ontologyTerm.getAttributes().keySet()) {
                node.addAttribute("attributes_" + key, ontologyTerm.getAttributes().get(key));
            }
        }
        return node;
    }

//    public static Node newNode(long uid, Disorder disorder) {
//        // IMPORTANT: phenotype nodes and relations must be created by the caller of this function!!!
//
//        Node node = new Node(uid, disorder.getId(), disorder.getName(), Node.Type.DISORDER);
//        node.addAttribute("description", disorder.getDescription());
//        node.addAttribute("source", disorder.getSource());
//        if (MapUtils.isNotEmpty(disorder.getAttributes())) {
//            for (String key : disorder.getAttributes().keySet()) {
//                node.addAttribute("attributes_" + key, disorder.getAttributes().get(key));
//            }
//        }
//        return node;
//    }

//    public static Node newNode(long uid, Individual individual) {
//        // IMPORTANT: father, mother, phenotypes, disorders, samples nodes and relations must be created by the caller of this
//        // function!!!
//
//        Node node = new Node(uid, individual.getId(), individual.getName(), Node.Type.INDIVIDUAL);
//        node.addAttribute("uuid", individual.getUuid());
//        if (individual.getLocation() != null) {
//            node.addAttribute("location_address", individual.getLocation().getAddress());
//            node.addAttribute("location_city", individual.getLocation().getCity());
//            node.addAttribute("location_postalCode", individual.getLocation().getPostalCode());
//            node.addAttribute("location_state", individual.getLocation().getState());
//            node.addAttribute("location_country", individual.getLocation().getCountry());
//        }
//        if (individual.getSex() != null) {
//            node.addAttribute("sex", individual.getSex().name());
//        }
//        if (individual.getKaryotypicSex() != null) {
//            node.addAttribute("karyotypicSex", individual.getKaryotypicSex().name());
//        }
//        node.addAttribute("ethnicity", individual.getEthnicity());
//        if (individual.getPopulation() != null) {
//            node.addAttribute("population_name", individual.getPopulation().getName());
//            node.addAttribute("population_subpopulation", individual.getPopulation().getSubpopulation());
//            node.addAttribute("population_description", individual.getPopulation().getDescription());
//        }
////        if (individual.getMultiples() != null) {
////            node.addAttribute("multiples_type", individual.getMultiples().getType());
////            node.addAttribute("multiples_siblings", StringUtils.join(individual.getMultiples().getSiblings(), ","));
////        }
//        node.addAttribute("dateOfBirth", individual.getDateOfBirth());
//        node.addAttribute("release", individual.getRelease());
//        node.addAttribute("version", individual.getRelease());
//        node.addAttribute("creationDate", individual.getCreationDate());
//        node.addAttribute("modificationDate", individual.getModificationDate());
//        addStatus(individual.getStatus(), node);
//        if (individual.getLifeStatus() != null) {
//            node.addAttribute("lifeStatus", individual.getLifeStatus().name());
//        }
//        node.addAttribute("parentalConsanguinity", individual.isParentalConsanguinity());
//        if (MapUtils.isNotEmpty(individual.getAttributes())) {
//            for (String key : individual.getAttributes().keySet()) {
//                node.addAttribute("attributes_" + key, individual.getAttributes().get(key).toString());
//            }
//        }
//        return node;
//    }
//
//    public static Node newNode(long uid, Sample sample) {
//        // IMPORTANT: phenotypes nodes and relations must be created by the caller of this function!!!
//
//        Node node = new Node(uid, sample.getId(), sample.getId(), Node.Type.SAMPLE);
//        node.addAttribute("uuid", sample.getUuid());
//        if (sample.getProcessing() != null) {
//            node.addAttribute("processing_product", sample.getProcessing().getProduct());
//            node.addAttribute("processing_preparationMethod", sample.getProcessing().getPreparationMethod());
//            node.addAttribute("processing_extractionMethod", sample.getProcessing().getExtractionMethod());
//            node.addAttribute("processing_labSampleId", sample.getProcessing().getLabSampleId());
//            node.addAttribute("processing_quantity", sample.getProcessing().getQuantity());
//            node.addAttribute("processing_date", sample.getProcessing().getDate());
//            // TODO: sample.getProcessing().getAttributes()
//        }
//        if (sample.getCollection() != null) {
//            node.addAttribute("collection_tissue", sample.getCollection().getTissue());
//            node.addAttribute("collection_organ", sample.getCollection().getOrgan());
//            node.addAttribute("collection_quantity", sample.getCollection().getQuantity());
//            node.addAttribute("collection_method", sample.getCollection().getMethod());
//            node.addAttribute("collection_date", sample.getCollection().getDate());
//            // TODO: sample.getCollection().getAttributes()
//        }
//        node.addAttribute("release", sample.getRelease());
//        node.addAttribute("version", sample.getRelease());
//        node.addAttribute("creationDate", sample.getCreationDate());
//        node.addAttribute("modificationDate", sample.getModificationDate());
//        addStatus(sample.getStatus(), node);
//        node.addAttribute("description", sample.getDescription());
////        node.addAttribute("type", sample.getType());
//        node.addAttribute("somatic", sample.isSomatic());
//        if (MapUtils.isNotEmpty(sample.getAttributes())) {
//            for (String key : sample.getAttributes().keySet()) {
//                node.addAttribute("attributes_" + key, sample.getAttributes().get(key).toString());
//            }
//        }
//        return node;
//    }
//
//    public static Node newNode(long uid, Family family) {
//        // IMPORTANT: phenotypes, disorder and members, nodes and relations must be created by the caller!
//
//        Node node = new Node(uid, family.getId(), family.getName(), Node.Type.FAMILY);
//        node.addAttribute("uuid", family.getUuid());
//        node.addAttribute("creationDate", family.getCreationDate());
//        node.addAttribute("modificationDate", family.getModificationDate());
//        addStatus(family.getStatus(), node);
//        node.addAttribute("expectedSize", family.getExpectedSize());
//        node.addAttribute("description", family.getDescription());
//        node.addAttribute("release", family.getRelease());
//        node.addAttribute("version", family.getRelease());
//        if (MapUtils.isNotEmpty(family.getAttributes())) {
//            for (String key : family.getAttributes().keySet()) {
//                node.addAttribute("attributes_" + key, family.getAttributes().get(key).toString());
//            }
//        }
//        return node;
//    }

    public static Node newNode(long uid, DiseasePanel.GenePanel panelGene) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelGene.getId(), panelGene.getName(), Node.Type.PANEL_GENE);
        addDiseasePanelCommon(panelGene, node);
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.VariantPanel panelVariant) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelVariant.getId(), panelVariant.getId(), Node.Type.PANEL_VARIANT);
        node.addAttribute("alternate", panelVariant.getAlternate());
        node.addAttribute("reference", panelVariant.getReference());
        addDiseasePanelCommon(panelVariant, node);
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.STR panelStr) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelStr.getId(), panelStr.getId(), Node.Type.PANEL_STR);
        node.addAttribute("repeatedSequence", panelStr.getRepeatedSequence());
        node.addAttribute("normalRepeats", String.valueOf(panelStr.getNormalRepeats()));
        node.addAttribute("pathogenicRepeats", String.valueOf(panelStr.getPathogenicRepeats()));
        addDiseasePanelCommon(panelStr, node);
        return node;
    }

    public static Node newNode(long uid, DiseasePanel.RegionPanel panelRegion) {
        // IMPORTANT: phenotypes nodes and relations must be created by the caller!

        Node node = new Node(uid, panelRegion.getId(), panelRegion.getId(), Node.Type.PANEL_REGION);
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

//    public static Node newNode(long uid, File file) {
//        // IMPORTANT: software, experiment and sample nodes and relations must be created by the caller!
//
//        Node node = new Node(uid, file.getId(), file.getName(), Node.Type.FILE);
//        node.addAttribute("uuid", file.getUuid());
//        if (file.getType() != null) {
//            node.addAttribute("type", file.getType().name());
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
//        Node node = new Node(uid, "", "", Node.Type.EXPERIMENT);
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
            node.addAttribute("publications", StringUtils.join(common.getPublications(), ","));
        }
        if (CollectionUtils.isNotEmpty(common.getCoordinates())) {
            node.addAttribute("coordinates", common.getCoordinates().stream().map(c -> c.getAssembly() + "+" + c.getLocation() + "+"
                    + c.getSource()).collect(Collectors.joining(",")));
        }
    }
}
