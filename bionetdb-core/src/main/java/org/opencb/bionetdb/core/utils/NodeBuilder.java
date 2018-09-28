package org.opencb.bionetdb.core.utils;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.*;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.TranscriptTfbs;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.utils.ListUtils;

import java.util.List;
import java.util.Map;

public class NodeBuilder {

    public static final String CHROMOSOME = "chromosome";
    public static final String START = "start";
    public static final String END = "end";
    public static final String REFERENCE = "reference";
    public static final String ALTERNATE = "alternate";
    public static final String STRAND = "strand";
    public static final String TYPE = "type";


    public static Node newNode(long uid, Variant variant) {
        Node node = new Node(uid, variant.toString(), variant.getId(), Node.Type.VARIANT);
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

    public static Node newNode(long uid, StudyEntry studyEntry, Node variantNode) {
        Node node = new Node(uid, variantNode.getId() + "_" + variantNode.getSource(), "", Node.Type.VARIANT_FILE_INFO);
        Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();
        node.addAttribute("filename", studyEntry.getFiles().get(0).getFileId());
        for (String key: fileAttrs.keySet()) {
            node.addAttribute(key, fileAttrs.get(key));
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
        node.addAttribute("source", gene.getSource());
        node.addAttribute("status", gene.getStatus());
        return node;
    }

    public static Node newNode(long uid, GeneDrugInteraction drug) {
        Node node = new Node(uid, null, drug.getDrugName(), Node.Type.DRUG);
        node.addAttribute("source", drug.getSource());
        node.addAttribute("type", drug.getType());
        node.addAttribute("studyType", drug.getStudyType());
        return node;
    }

    public static Node newNode(long uid, GeneTraitAssociation disease) {
        Node node = new Node(uid, disease.getId(), disease.getName(), Node.Type.DISEASE);
        node.addAttribute("hpo", disease.getHpo());
        node.addAttribute("numberOfPubmeds", disease.getNumberOfPubmeds());
        node.addAttribute("score", disease.getScore());
        node.addAttribute("source", disease.getSource());
        if (ListUtils.isNotEmpty(disease.getSources())) {
            node.addAttribute("sources", StringUtils.join(disease.getSources(), ","));
        }
        if (ListUtils.isNotEmpty(disease.getAssociationTypes())) {
            node.addAttribute("associationTypes", StringUtils.join(disease.getAssociationTypes(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, Transcript transcript) {
        Node node = new Node(uid, transcript.getId(), transcript.getName(), Node.Type.TRANSCRIPT);
        node.addAttribute("proteinId", transcript.getProteinID());
        node.addAttribute("biotype", transcript.getBiotype());
        node.addAttribute("chromosome", transcript.getChromosome());
        node.addAttribute("start", transcript.getStart());
        node.addAttribute("end", transcript.getEnd());
        node.addAttribute("strand", transcript.getStrand());
        node.addAttribute("status", transcript.getStatus());
        node.addAttribute("cdnaCodingStart", transcript.getCdnaCodingStart());
        node.addAttribute("cdnaCodingEnd", transcript.getCdnaCodingEnd());
        node.addAttribute("genomicCodingStart", transcript.getGenomicCodingStart());
        node.addAttribute("genomicCodingEnd", transcript.getGenomicCodingEnd());
        node.addAttribute("cdsLength", transcript.getCdsLength());
        node.addAttribute("description", transcript.getDescription());
        if (ListUtils.isNotEmpty(transcript.getAnnotationFlags())) {
            node.addAttribute("annotationFlags", StringUtils.join(transcript.getAnnotationFlags(), ","));
        }
        return node;
    }

    public static Node newNode(long uid, TranscriptTfbs tfbs) {
        Node node = new Node(uid, null, tfbs.getTfName(), Node.Type.TFBS);
        node.addAttribute("chromosome", tfbs.getChromosome());
        node.addAttribute("start", tfbs.getStart());
        node.addAttribute("end", tfbs.getEnd());
        node.addAttribute("strand", tfbs.getStrand());
        node.addAttribute("relativeStart", tfbs.getRelativeStart());
        node.addAttribute("relativeEnd", tfbs.getRelativeEnd());
        node.addAttribute("score", tfbs.getScore());
        node.addAttribute("pwm", tfbs.getPwm());
        return node;
    }

    public static Node newNode(long uid, Xref xref) {
        Node node = new Node(uid, xref.getId(), null, Node.Type.XREF);
        node.addAttribute("dbName", xref.getDbName());
        node.addAttribute("dbDisplayName", xref.getDbDisplayName());
        node.addAttribute("description", xref.getDescription());
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
            for (EvidenceType evidenceType: protein.getEvidence()) {
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

    public static Node newNode(long uid, DbReferenceType xref) {
        Node node = new Node(uid, xref.getId(), null, Node.Type.XREF);
        node.addAttribute("dbName", xref.getType());
        return node;
    }
/*
    public static Node newNode(long uid, Panel panel) {
        Node node = new Node(uid, panel.getId(), panel.getName(), Node.Type.PANEL);
        node.addAttribute("author", panel.getAuthor());
        node.addAttribute("version", panel.getVersion());
        node.addAttribute("date", panel.getDate());
        if (panel.getSource() != null) {
            node.addAttribute("sourceProject", panel.getSource().getProject());
            node.addAttribute("sourceId", panel.getSource().getId());
            node.addAttribute("sourceVersion", panel.getSource().getVersion());
        }
        return node;
    }

    public static Node newNode(long uid, OntologyTerm term) {
        Node node = new Node(uid, term.getId(), term.getName(), Node.Type.ONTOLOGY);
        node.addAttribute("source", term.getSource());
        return node;
    }
    */
}
