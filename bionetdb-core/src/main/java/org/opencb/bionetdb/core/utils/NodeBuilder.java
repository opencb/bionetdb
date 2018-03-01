package org.opencb.bionetdb.core.utils;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.utils.ListUtils;

import java.util.List;
import java.util.Map;

public class NodeBuilder {
    public static Node newNode(Variant variant) {
        Node node = new Node(-1, variant.getId(), variant.toString(), Node.Type.VARIANT);
        node.addAttribute("chromosome", variant.getChromosome());
        node.addAttribute("start", variant.getStart());
        node.addAttribute("end", variant.getEnd());
        node.addAttribute("reference", variant.getReference());
        node.addAttribute("alternate", variant.getAlternate());
        node.addAttribute("strand", variant.getStrand());
        node.addAttribute("type", variant.getType().toString());

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

    public static Node newNode(StudyEntry studyEntry, Node variantNode) {
        Node node = new Node(-1, variantNode.getId() + "_" + variantNode.getSource(), "", Node.Type.VARIANT_FILE_INFO);
        Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();
        node.addAttribute("filename", studyEntry.getFiles().get(0).getFileId());
        for (String key: fileAttrs.keySet()) {
            node.addAttribute(key, fileAttrs.get(key));
        }
        return node;
    }

    public static Node newCallNode(List<String> formatKeys, List<String> formatValues) {
        Node node = new Node(-1, formatValues.get(0), formatValues.get(0), Node.Type.VARIANT_CALL);
        for (int i = 0; i < formatKeys.size(); i++) {
            node.addAttribute(formatKeys.get(i), formatValues.get(i));
        }
        return node;
    }

    public static Node newNode(PopulationFrequency popFreq) {
        Node node = new Node(-1, popFreq.getPopulation(), popFreq.getPopulation(), Node.Type.POPULATION_FREQUENCY);
        node.addAttribute("study", popFreq.getStudy());
        node.addAttribute("population", popFreq.getPopulation());
        node.addAttribute("refAlleleFreq", popFreq.getRefAlleleFreq());
        node.addAttribute("altAlleleFreq", popFreq.getAltAlleleFreq());
        return node;
    }

    public static Node newNode(Score score, Node.Type nodeType) {
        Node node = new Node(-1, score.getSource(), null, nodeType);
        node.addAttribute("score", score.getScore());
        node.addAttribute("source", score.getSource());
        node.addAttribute("description", score.getDescription());
        return node;
    }

    public static Node newNode(EvidenceEntry evidence, Node.Type nodeType) {
        Node node = new Node(-1, null, null, nodeType);
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

    public static Node newNode(ConsequenceType ct) {
        Node node = new Node(-1, ct.getBiotype(), null, Node.Type.CONSEQUENCE_TYPE);
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

    public static Node newNode(ProteinVariantAnnotation annotation) {
        Node node = new Node(-1, annotation.getUniprotAccession(), annotation.getUniprotName(),
                Node.Type.PROTEIN_VARIANT_ANNOTATION);
        node.addAttribute("position", annotation.getPosition());
        node.addAttribute("reference", annotation.getReference());
        node.addAttribute("alternate", annotation.getAlternate());
        node.addAttribute("functionalDescription", annotation.getFunctionalDescription());
        return node;
    }

    public static Node newNode(ProteinFeature feature) {
        Node node = new Node(-1, null, feature.getId(), Node.Type.PROTEIN_FEATURE);
        node.addAttribute("start", feature.getStart());
        node.addAttribute("end", feature.getEnd());
        node.addAttribute("type", feature.getType());
        node.addAttribute("description", feature.getDescription());
        return node;
    }
}
