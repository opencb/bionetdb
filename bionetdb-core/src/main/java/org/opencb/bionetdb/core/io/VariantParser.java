package org.opencb.bionetdb.core.io;

import org.apache.commons.collections.MapUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.commons.utils.ListUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VariantParser {
    private long uidCounter = 0;
    private Network network;

    private ArrayList<String> sampleNames;
    private Map<String, Node> sampleNodeMap;

    public VariantParser() {
        this(0);
    }

    public VariantParser(long uidCounter) {
        this.uidCounter = uidCounter;
        network = new Network();
        sampleNodeMap = new HashMap<>();
    }

    public Network parse(List<Variant> variants) {
        int vCounter = 1;
        for (Variant variant: variants) {
            parse(variant);
            System.out.println(vCounter++ + ": Variant : " + variant.getId()
                    + ", total nodes = " + Node.getCounter() + ", total relations = " + Relation.getCounter());
        }
        return network;
    }

    public Network parse(Variant variant) {
        if (variant != null) {
            // main node
            Node vNode = new Node(uidCounter++, variant.getId(), variant.toString(), Node.Type.VARIANT);
            vNode.addAttribute("chromosome", variant.getChromosome());
            vNode.addAttribute("start", variant.getStart());
            vNode.addAttribute("end", variant.getEnd());
            vNode.addAttribute("reference", variant.getReference());
            vNode.addAttribute("alternate", variant.getAlternate());
            vNode.addAttribute("strand", variant.getStrand());
            vNode.addAttribute("type", variant.getType().toString());
            network.addNode(vNode);

//            if (ListUtils.isNotEmpty(variant.getStudies())) {
//                // Only one single study is supported
//                StudyEntry studyEntry = variant.getStudies().get(0);
//
//                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
//                    String source = studyEntry.getFiles().get(0).getFileId();
//                    if (org.apache.commons.lang.StringUtils.isNotEmpty(source)) {
//                        vNode.setSource(source);
//                    }
//
//                    // Create the variant call info node adding all file attributes (FILTER, QUAL, INFO fields...)
//                    Node fileInfoNode = new Node(uidCounter++, vNode.getId() + "_" + source, "", Node.Type.VARIANT_FILE_INFO);
//                    Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();
//                    fileInfoNode.addAttribute("filename", studyEntry.getFiles().get(0).getFileId());
//                    for (String key: fileAttrs.keySet()) {
//                        fileInfoNode.addAttribute(key, fileAttrs.get(key));
//                    }
//                    network.addNode(fileInfoNode);
//
//                    for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
//                        // Create the sample node
//                        if (!sampleNodeMap.containsKey(sampleNames.get(i))) {
//                            System.out.println("Internal error: sample name " + sampleNames.get(i) + " does not exist!!!");
//                        }
//                        Node sampleNode = sampleNodeMap.get(sampleNames.get(i));
////                        if (!sampleUidMap.containsKey(sampleNames.get(i))) {
////                            sampleNode = new Node(uidCounter++, sampleNames.get(i), sampleNames.get(i), Node.Type.SAMPLE);
////                            network.addNode(sampleNode);
////
////                            sampleUidMap.put(sampleNode.getId(), sampleNode.getUid());
////                        } else {
////                            sampleNode = new Node(sampleUidMap.get(sampleNames.get(i)));
////                        }
//
//                        // And the call node for that sample adding the format attributes
//                        Node callNode = new Node(uidCounter++, studyEntry.getSampleData(i).get(0), studyEntry.getSampleData(i).get(0),
//                                Node.Type.VARIANT_CALL);
//                        for (int j = 0; j < studyEntry.getSamplesData().get(i).size(); j++) {
//                            callNode.addAttribute(studyEntry.getFormat().get(j), studyEntry.getSampleData(i).get(j));
//                        }
//                        network.addNode(callNode);
//
//                        // Relation: sample - variant call
//                        Relation sVCallRel = new Relation(uidCounter++, sampleNode.getId() + callNode.getId(), sampleNode.getUid(),
//                                callNode.getUid(), Relation.Type.VARIANT_CALL);
//                        network.addRelation(sVCallRel);
//
//                        // Relation: variant call - variant file info
//                        Relation vFileInfoRel = new Relation(uidCounter++, callNode.getId() + fileInfoNode.getId(), callNode.getUid(),
//                                fileInfoNode.getUid(), Relation.Type.VARIANT_FILE_INFO);
//                        network.addRelation(vFileInfoRel);
//
//                        // Relation: variant - variant call
//                        Relation vCallRel = new Relation(uidCounter++, vNode.getId() + callNode.getId(), vNode.getUid(),
//                                callNode.getUid(), Relation.Type.VARIANT_CALL);
//                        network.addRelation(vCallRel);
//                    }
//                }
//            }

            if (variant.getAnnotation() != null) {
                // Annotation node
                Node annotNode = new Node(uidCounter++, "VariantAnnotation", null, Node.Type.VARIANT_ANNOTATION);
                network.addNode(annotNode);

                // Relation: variant - annotation
                Relation vAnnotRel = new Relation(uidCounter++, vNode.getId() + annotNode.getId(), vNode.getUid(), annotNode.getUid(),
                        Relation.Type.ANNOTATION);
                network.addRelation(vAnnotRel);

                if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {

                    // internal management for Proteins
                    Map<String, List<Node>> mapUniprotVANode = new HashMap<>();


                    // consequence type nodes
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        if (ct.getBiotype() == null) {
                            continue;
                        }
                        Node ctNode = new Node(uidCounter++, "ConsequenceType_" + (uidCounter++), null, Node.Type.CONSEQUENCE_TYPE);
                        ctNode.addAttribute("biotype", ct.getBiotype());
                        if (ListUtils.isNotEmpty(ct.getTranscriptAnnotationFlags())) {
                            ctNode.addAttribute("transcriptAnnotationFlags", String.join(",", ct.getTranscriptAnnotationFlags()));
                        }
                        ctNode.addAttribute("cdnaPosition", ct.getCdnaPosition());
                        ctNode.addAttribute("cdsPosition", ct.getCdsPosition());
                        ctNode.addAttribute("codon", ct.getCodon());
                        network.addNode(ctNode);

                        // Relation: variant - consequence type
                        Relation vCtRel = new Relation(uidCounter++, annotNode.getId() + ctNode.getId(), annotNode.getUid(),
                                ctNode.getUid(), Relation.Type.CONSEQUENCE_TYPE);
                        network.addRelation(vCtRel);

                        // Transcript nodes
                        if (ct.getEnsemblTranscriptId() != null) {
                            Node transcriptNode = new Node(uidCounter++, "Ensembl:" + ct.getEnsemblTranscriptId(), null,
                                    Node.Type.TRANSCRIPT);
                            network.addNode(transcriptNode);

                            // Relation: consequence type - transcript
                            Relation ctTRel = new Relation(uidCounter++, ctNode.getId() + transcriptNode.getId(), ctNode.getUid(),
                                    transcriptNode.getUid(), Relation.Type.TRANSCRIPT);
                            network.addRelation(ctTRel);

                            // Ensembl gene node
                            if (ct.getEnsemblGeneId() != null) {
                                Node eGeneNode = new Node(uidCounter++, "Ensembl:" + ct.getEnsemblGeneId(), ct.getGeneName(),
                                        Node.Type.GENE);
                                eGeneNode.addAttribute("ensemblGeneId", ct.getEnsemblGeneId());
                                //xrefEGeneNode.setSubtypes(Collections.singletonList(Node.Type.GENE));
                                network.addNode(eGeneNode);

                                // Relation: transcript - ensembl gene
                                Relation tEgRel = new Relation(uidCounter++, transcriptNode.getId() + eGeneNode.getId(),
                                        transcriptNode.getUid(),
                                        eGeneNode.getUid(), Relation.Type.GENE);
                                network.addRelation(tEgRel);
                            }

                            //
                            // Xref managements
                            //

                            // Xref ensembl transcript node
                            Node xrefETranscriptNode = new Node(uidCounter++, ct.getEnsemblTranscriptId(), "", Node.Type.XREF);
                            network.addNode(xrefETranscriptNode);

                            // Relation: transcript - xref ensembl transcript
                            Relation tXEtRel = new Relation(uidCounter++, transcriptNode.getId() + xrefETranscriptNode.getId(),
                                    transcriptNode.getUid(), xrefETranscriptNode.getUid(), Relation.Type.XREF);
                            network.addRelation(tXEtRel);

                            // Xref ensembl gene node
                            Node xrefEGeneNode = new Node(uidCounter++, ct.getEnsemblGeneId(), "", Node.Type.XREF);
                            network.addNode(xrefEGeneNode);

                            // Relation: transcript - xref ensembl gene
                            Relation tXEgRel = new Relation(uidCounter++, transcriptNode.getId() + xrefEGeneNode.getId(),
                                    transcriptNode.getUid(), xrefEGeneNode.getUid(), Relation.Type.XREF);
                            network.addRelation(tXEgRel);

                            // Xref gene node
                            Node xrefGeneNode = new Node(uidCounter++, ct.getGeneName(), "", Node.Type.XREF);
                            network.addNode(xrefGeneNode);

                            // Relation: transcript - xref gene
                            Relation tXGRel = new Relation(uidCounter++, transcriptNode.getId() + xrefGeneNode.getId(),
                                    transcriptNode.getUid(), xrefGeneNode.getUid(), Relation.Type.XREF);
                            network.addRelation(tXGRel);
                        } else {
                            System.out.println("Transcript is NULL !!!");
                        }

                        // Protein variant annotation
                        if (ct.getProteinVariantAnnotation() != null) {
                            ProteinVariantAnnotation protVA = ct.getProteinVariantAnnotation();
                            // Create node
                            String protVANodeId;
                            if (protVA.getUniprotAccession() != null) {
                                protVANodeId = "ProteinAnnotation_uniprot:" + protVA.getUniprotAccession();
                            } else {
                                protVANodeId = "ProteinAnnotation_" + uidCounter++;
                            }
                            Node protVANode = new Node(uidCounter++, protVANodeId, protVA.getUniprotName(), Node.Type.PROTEIN_ANNOTATION);
                            protVANode.addAttribute("uniprotAccession", protVA.getUniprotAccession());
                            protVANode.addAttribute("uniprotName", protVA.getUniprotName());
                            protVANode.addAttribute("uniprotVariantId", protVA.getUniprotVariantId());
                            protVANode.addAttribute("functionalDescription", protVA.getFunctionalDescription());
                            if (ListUtils.isNotEmpty(protVA.getKeywords())) {
                                protVANode.addAttribute("keywords", String.join(",", protVA.getKeywords()));
                            }
                            protVANode.addAttribute("reference", protVA.getReference());
                            protVANode.addAttribute("alternate", protVA.getAlternate());
                            network.addNode(protVANode);

                            // And create relationship consequence type -> protein variation annotation
                            Relation ctTRel = new Relation(uidCounter++, ctNode.getId() + protVANode.getId(), ctNode.getUid(),
                                    protVANode.getUid(), Relation.Type.ANNOTATION);
                            network.addRelation(ctTRel);

                            // Check for protein features
                            if (ListUtils.isNotEmpty(protVA.getFeatures())) {
                                for (ProteinFeature protFeat : protVA.getFeatures()) {
                                    // ... and create node for each protein feature
                                    Node protFeatNode = new Node(uidCounter++);
                                    protFeatNode.setId(protFeat.getId() == null ? "ProteinFeature_" + (uidCounter++)
                                            : protFeat.getId());
                                    protFeatNode.setType(Node.Type.PROTEIN_FEATURE);
                                    protFeatNode.addAttribute("ftype", protFeat.getType());
                                    protFeatNode.addAttribute("description", protFeat.getDescription());
                                    protFeatNode.addAttribute("start", protFeat.getStart());
                                    protFeatNode.addAttribute("end", protFeat.getEnd());
                                    network.addNode(protFeatNode);

                                    // ... and its relationship
                                    Relation protVAFeatRel = new Relation(uidCounter++, protVANode.getId() + protFeatNode.getId(),
                                            protVANode.getUid(), protFeatNode.getUid(), Relation.Type.PROTEIN_FEATURE);
                                    network.addRelation(protVAFeatRel);
                                }
                            }

                            // Check for protein...
                            if (protVA.getUniprotAccession() != null) {
                                // internal management
                                if (!mapUniprotVANode.containsKey(protVA.getUniprotAccession())) {
                                    mapUniprotVANode.put(protVA.getUniprotAccession(), new ArrayList<>());
                                }
                                mapUniprotVANode.get(protVA.getUniprotAccession()).add(protVANode);

//                                // ... and create node for the protein
//                                Protein protein = new Protein();
//                                protein.setName(protVA.getUniprotAccession());
//                                protein.setXref(new Xref("uniprot", "", protVA.getUniprotAccession(), ""));
//                                network.addNode(protein);
//
//                                // ... and its relationship
//                                Relation protVARel = new Relation(protein.getId() + protVANode.getId(),
//                                        protein.getId(), protein.getType().toString(), protVANode.getId(),
//                                        protVANode.getType().toString(), Relation.Type.ANNOTATION);
//                                network.addRelation(protVARel);
                            }

                            // Check for substitution scores...
                            if (ListUtils.isNotEmpty(protVA.getSubstitutionScores())) {
                                for (Score score: protVA.getSubstitutionScores()) {
                                    // ... and create node for each substitution score
                                    Node substNode = new Node(uidCounter++, "SubstitutionScore_" + (uidCounter++), null,
                                            Node.Type.SUBSTITUTION_SCORE);
                                    substNode.addAttribute("score", score.getScore());
                                    substNode.addAttribute("source", score.getSource());
                                    substNode.addAttribute("description", score.getDescription());
                                    network.addNode(substNode);

                                    // ... and its relationship
                                    Relation protVASubstRel = new Relation(uidCounter++, protVANode.getId() + substNode.getId(),
                                            protVANode.getUid(), substNode.getUid(), Relation.Type.SUBST_SCORE);
                                    network.addRelation(protVASubstRel);
                                }
                            }
                        }

                        // Sequence Ontology terms
                        if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                            // Sequence Ontology term nodes
                            for (SequenceOntologyTerm sot: ct.getSequenceOntologyTerms()) {
                                Node soNode = new Node(uidCounter++, sot.getAccession(), sot.getName(), Node.Type.SO);
                                soNode.addAttribute("accession", sot.getAccession());
                                network.addNode(soNode);

                                // Relation: consequence type - so
                                Relation ctSoRel = new Relation(uidCounter++, ctNode.getId() + soNode.getId(), ctNode.getUid(),
                                        soNode.getUid(), Relation.Type.SO);
                                network.addRelation(ctSoRel);
                            }
                        }
                    }
                    if (MapUtils.isNotEmpty(mapUniprotVANode)) {
                        network.getAttributes().put("_uniprot", mapUniprotVANode);
                    }
                }

//                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
//                    for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
//                        Node popFreqNode = new Node(uidCounter++, "PopulationFrequency", null, Node.Type.POPULATION_FREQUENCY);
//                        popFreqNode.addAttribute("study", popFreq.getStudy());
//                        popFreqNode.addAttribute("population", popFreq.getPopulation());
//                        popFreqNode.addAttribute("refAlleleFreq", popFreq.getRefAlleleFreq());
//                        popFreqNode.addAttribute("altAlleleFreq", popFreq.getAltAlleleFreq());
//                        network.addNode(popFreqNode);
//
//                        // Relation: variant - population frequency
//                        Relation vPfRel = new Relation(uidCounter++, annotNode.getId() + popFreqNode.getId(), annotNode.getUid(),
//                                popFreqNode.getUid(), Relation.Type.POPULATION_FREQUENCY);
//                        network.addRelation(vPfRel);
//
//                    }
//                }
//
//                // Conservation
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
//                    for (Score score : variant.getAnnotation().getConservation()) {
//                        Node conservNode = new Node(uidCounter++, "Conservation", null, Node.Type.CONSERVATION);
//                        conservNode.addAttribute("score", score.getScore());
//                        conservNode.addAttribute("source", score.getSource());
//                        conservNode.addAttribute("description", score.getDescription());
//                        network.addNode(conservNode);
//
//                        // Relation: variant - conservation
//                        Relation vConservRel = new Relation(uidCounter++, annotNode.getId() + conservNode.getId(), annotNode.getUid(),
//                                conservNode.getUid(), Relation.Type.CONSERVATION);
//                        network.addRelation(vConservRel);
//                    }
//                }
//
//                // Trait association
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
//                    for (EvidenceEntry evidence : variant.getAnnotation().getTraitAssociation()) {
//                        Node evNode = new Node(uidCounter++, "TraitAssociation", null, Node.Type.TRAIT_ASSOCIATION);
//                        if (evidence.getSource() != null && evidence.getSource().getName() != null) {
//                            evNode.addAttribute("source", evidence.getSource().getName());
//                        }
//                        evNode.addAttribute("url", evidence.getUrl());
//                        if (ListUtils.isNotEmpty(evidence.getHeritableTraits())) {
//                            StringBuilder her = new StringBuilder();
//                            for (HeritableTrait heritableTrait : evidence.getHeritableTraits()) {
//                                if (her.length() > 0) {
//                                    her.append(",");
//                                }
//                                her.append(heritableTrait.getTrait());
//                            }
//                            evNode.addAttribute("heritableTraits", her.toString());
//                        }
//                        if (evidence.getSource() != null && evidence.getSource().getName() != null) {
//                            evNode.addAttribute("source", evidence.getSource().getName());
//                        }
//                        if (ListUtils.isNotEmpty(evidence.getAlleleOrigin())) {
//                            StringBuilder alleleOri = new StringBuilder();
//                            for (AlleleOrigin alleleOrigin : evidence.getAlleleOrigin()) {
//                                if (alleleOri.length() > 0 && alleleOrigin.name() != null) {
//                                    alleleOri.append(",");
//                                }
//                                alleleOri.append(alleleOrigin.name());
//                            }
//                            evNode.addAttribute("alleleOrigin", alleleOri.toString());
//                        }
//                        network.addNode(evNode);
//
//                        // Relation: variant - conservation
//                        Relation vFuncRel = new Relation(uidCounter++, annotNode.getId() + evNode.getId(), annotNode.getUid(),
//                                evNode.getUid(), Relation.Type.TRAIT_ASSOCIATION);
//                        network.addRelation(vFuncRel);
//
//                    }
//                }
//
//                // Functional score
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
//                    for (Score score : variant.getAnnotation().getFunctionalScore()) {
//                        Node funcNode = new Node(uidCounter++, "FunctionalScore", null, Node.Type.FUNCTIONAL_SCORE);
//                        funcNode.addAttribute("score", score.getScore());
//                        funcNode.addAttribute("source", score.getSource());
//                        network.addNode(funcNode);
//
//                        // Relation: variant - conservation
//                        Relation vTraitRel = new Relation(uidCounter++, annotNode.getId() + funcNode.getId(), annotNode.getUid(),
//                                funcNode.getUid(), Relation.Type.FUNCTIONAL_SCORE);
//                        network.addRelation(vTraitRel);
//
//                    }
//                }
            }
        }
        return network;
    }

    public void setSampleNames(ArrayList<String> sampleNames) {
        this.sampleNames = sampleNames;
        for (String sampleName: sampleNames) {
            Node sampleNode = new Node(uidCounter++, sampleName, sampleName, Node.Type.SAMPLE);
            network.addNode(sampleNode);

            sampleNodeMap.put(sampleName, sampleNode);
        }
    }

    public long getUidCounter() {
        return uidCounter;
    }

    public VariantParser setUidCounter(long uidCounter) {
        this.uidCounter = uidCounter;
        return this;
    }
}
