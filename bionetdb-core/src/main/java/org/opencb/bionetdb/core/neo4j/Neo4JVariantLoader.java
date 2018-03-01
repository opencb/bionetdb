package org.opencb.bionetdb.core.neo4j;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.utils.ListUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Neo4JVariantLoader {

    private Neo4JNetworkDBAdaptor networkDBAdaptor;
    private List<String> sampleNames;

    private static final int VARIANT_BATCH_SIZE = 10000;

    public Neo4JVariantLoader(Neo4JNetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public void loadVCFFile(Path path) {
        // VCF File reader management
        VcfFileReader vcfFileReader = new VcfFileReader(path.toString(), false);
        vcfFileReader.open();
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        sampleNames = vcfHeader.getSampleNamesInOrder();

        // VariantContext-to-Variant converter
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter("dataset",
                path.toFile().getName(), vcfFileReader.getVcfHeader().getSampleNamesInOrder());

        List<VariantContext> variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        while (variantContexts.size() == VARIANT_BATCH_SIZE) {
            // Convert to variants and load them into the network database
            loadVariants(convert(variantContexts, converter));

            // Read next batch
            variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        }
        // Process the remaining variants
        if (variantContexts.size() > 0) {
            // Convert to variants and load them into the network database
            loadVariants(convert(variantContexts, converter));
        }

        // close VCF file reader
        vcfFileReader.close();
    }

    public void loadVariants(List<Variant> variants) {
        Session session = networkDBAdaptor.getDriver().session();
        for (Variant variant: variants) {
            session.writeTransaction(tx -> {
                loadVariant(variant, tx);
                return 1;
            });
        }
        session.close();
    }


    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void loadVariant(Variant variant, Transaction tx) {
        if (variant != null) {
            if (StringUtils.isEmpty(variant.getId()) || ".".equals(variant.getId())) {
                return;
            }

            // Variant node
            Node variantNode = NodeBuilder.newNode(variant);
            networkDBAdaptor.mergeNode(variantNode, "id", tx);

            // Sample management
            if (ListUtils.isNotEmpty(variant.getStudies())) {
                // Only one single study is supported
                StudyEntry studyEntry = variant.getStudies().get(0);

                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                    // Create the variant call info node adding all file attributes (FILTER, QUAL, INFO fields...)
                    Node fileEntryNode = NodeBuilder.newNode(variant.getStudies().get(0), variantNode);
                    networkDBAdaptor.addNode(fileEntryNode, tx);

                    for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
                        // Create the sample node
                        Node sampleNode = new Node(-1, sampleNames.get(i), sampleNames.get(i), Node.Type.SAMPLE);
                        networkDBAdaptor.mergeNode(sampleNode, "id", tx);

                        // And the call node for that sample adding the format attributes
                        Node callNode = NodeBuilder.newCallNode(studyEntry.getFormat(), studyEntry.getSampleData(i));
                        networkDBAdaptor.addNode(callNode, tx);

                        // Relation: sample - variant call
                        Relation sVCallRel = new Relation(-1, sampleNode.getId() + "_" + callNode.getId(), sampleNode.getUid(),
                                callNode.getUid(), Relation.Type.VARIANT_CALL);
                        networkDBAdaptor.mergeRelation(sVCallRel, tx);

                        // Relation: variant call - variant file info
                        Relation vFileInfoRel = new Relation(-1, callNode.getId() + "_" + fileEntryNode.getId(), callNode.getUid(),
                                fileEntryNode.getUid(), Relation.Type.VARIANT_FILE_INFO);
                        networkDBAdaptor.mergeRelation(vFileInfoRel, tx);

                        // Relation: variant - variant call
                        Relation vCallRel = new Relation(-1, null, variantNode.getUid(),
                                callNode.getUid(), Relation.Type.VARIANT_CALL);
                        networkDBAdaptor.addRelation(vCallRel, tx);
                    }
                }
            }

            // Variant annotation
            if (variant.getAnnotation() != null) {
                // Consequence types
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                    // Consequence type nodes
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        Node ctNode = NodeBuilder.newNode(ct);
                        networkDBAdaptor.addNode(ctNode, tx);

                        // Relation: variant - consequence type
                        Relation vCtRel = new Relation(-1, null, variantNode.getUid(), ctNode.getUid(),
                                Relation.Type.CONSEQUENCE_TYPE);
                        networkDBAdaptor.mergeRelation(vCtRel, tx);


                        // SO
                        if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                            for (SequenceOntologyTerm so: ct.getSequenceOntologyTerms()) {
                                Node soNode = new Node(-1, so.getAccession(), so.getName(), Node.Type.SO);
                                networkDBAdaptor.mergeNode(soNode, "id", tx);

                                // Relation: consequence type - so
                                Relation ctSoRel = new Relation(-1, null, ctNode.getUid(), soNode.getUid(),
                                        Relation.Type.SO);
                                networkDBAdaptor.mergeRelation(ctSoRel, tx);
                            }
                        }

                        // Protein annotation: substitution scores, keywords and features
                        if (ct.getProteinVariantAnnotation() != null) {
                            // Protein variant annotation node
                            Node annotNode = NodeBuilder.newNode(ct.getProteinVariantAnnotation());
                            networkDBAdaptor.addNode(annotNode, tx);

                            // Relation: consequence type - protein variant annotation
                            Relation ctAnnotRel = new Relation(-1, null, ctNode.getUid(), annotNode.getUid(),
                                    Relation.Type.PROTEIN_VARIANT_ANNOTATION);
                            networkDBAdaptor.mergeRelation(ctAnnotRel, tx);

                            // Protein relationship management
                            if (ct.getProteinVariantAnnotation().getUniprotAccession() != null) {
                                String uniprotId = ct.getProteinVariantAnnotation().getUniprotAccession();
                                String uniprotName = ct.getProteinVariantAnnotation().getUniprotName();
                                StringBuilder cypher = new StringBuilder();
                                cypher.append("MATCH (n:PROTEIN)-[rx:XREF]->(x:XREF{attr_source:'uniprot'}) WHERE x.id = '")
                                        .append(uniprotId).append("' RETURN n");
                                List<Node> proteinNodes = null;
                                try {
                                    proteinNodes = networkDBAdaptor.nodeQuery(cypher.toString());
                                    if (ListUtils.isEmpty(proteinNodes)) {
                                        // This protein is not stored in the database, we must create the node and then
                                        // link to the protein variant annotation
                                        Node proteinNode = new Node(-1, uniprotId, uniprotName, Node.Type.PROTEIN);
                                        networkDBAdaptor.addNode(proteinNode, tx);

                                        proteinNodes.add(proteinNode);
                                    }
                                } catch (BioNetDBException e) {
                                    e.printStackTrace();
                                }

                                // Link protein nodes to the protein variant annotation
                                for (Node proteinNode: proteinNodes) {
                                    // Relation: protein - protein variant annotation
                                    Relation proteinAnnotRel = new Relation(-1, null, proteinNode.getUid(), annotNode.getUid(),
                                            Relation.Type.PROTEIN_VARIANT_ANNOTATION);
                                    networkDBAdaptor.mergeRelation(proteinAnnotRel, tx);
                                }
                            }

                            // Protein substitution scores
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                                for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                    Node subsNode = NodeBuilder.newNode(score, Node.Type.SUBSTITUTION_SCORE);
                                    networkDBAdaptor.addNode(subsNode, tx);

                                    // Relation: protein variant annotation - substitution score
                                    Relation ctSubsRel = new Relation(-1, null, annotNode.getUid(), subsNode.getUid(),
                                            Relation.Type.SUBSTITUTION_SCORE);
                                    networkDBAdaptor.mergeRelation(ctSubsRel, tx);
                                }
                            }

                            // Protein keywords
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getKeywords())) {
                                for (String keyword: ct.getProteinVariantAnnotation().getKeywords()) {
                                    Node kwNode = new Node(-1, null, keyword, Node.Type.PROTEIN_KEYWORD);
                                    networkDBAdaptor.mergeNode(kwNode, "name", tx);

                                    // Relation: protein variant annotation - so
                                    Relation ctKwRel = new Relation(-1, null, annotNode.getUid(), kwNode.getUid(),
                                            Relation.Type.PROTEIN_KEYWORD);
                                    networkDBAdaptor.mergeRelation(ctKwRel, tx);
                                }
                            }

                            // Protein features
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getFeatures())) {
                                for (ProteinFeature feature : ct.getProteinVariantAnnotation().getFeatures()) {
                                    Node featureNode = NodeBuilder.newNode(feature);
                                    networkDBAdaptor.addNode(featureNode, tx);

                                    // Relation: protein variant annotation - protein feature
                                    Relation ctFeatureRel = new Relation(-1, null, annotNode.getUid(), featureNode.getUid(),
                                            Relation.Type.PROTEIN_FEATURE);
                                    networkDBAdaptor.mergeRelation(ctFeatureRel, tx);
                                }
                            }
                        }

                        // Transcript
                        if (ct.getEnsemblTranscriptId() != null) {
                            Node transcriptNode = new Node(-1, ct.getEnsemblTranscriptId(), null, Node.Type.TRANSCRIPT);
                            networkDBAdaptor.mergeNode(transcriptNode, "id", tx);

                            // Relation: consequence type - transcript
                            Relation ctTranscriptRel = new Relation(-1, null, ctNode.getUid(), transcriptNode.getUid(),
                                    Relation.Type.TRANSCRIPT);
                            networkDBAdaptor.mergeRelation(ctTranscriptRel, tx);
                        }
                    }
                }

                // Population frequencies
                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                    for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                        Node popFreqNode = NodeBuilder.newNode(popFreq);
                        networkDBAdaptor.addNode(popFreqNode, tx);

                        // Relation: variant - population frequency
                        Relation vPfRel = new Relation(-1, null, variantNode.getUid(), popFreqNode.getUid(),
                                Relation.Type.POPULATION_FREQUENCY);
                        networkDBAdaptor.mergeRelation(vPfRel, tx);
                    }
                }

                // Conservation values
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                    for (Score score: variant.getAnnotation().getConservation()) {
                        Node consNode = NodeBuilder.newNode(score, Node.Type.CONSERVATION);
                        networkDBAdaptor.addNode(consNode, tx);

                        // Relation: variant - conservation
                        Relation vConsRel = new Relation(-1, null, variantNode.getUid(), consNode.getUid(),
                                Relation.Type.CONSERVATION);
                        networkDBAdaptor.mergeRelation(vConsRel, tx);
                    }
                }

                // Trait associations
                if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                    for (EvidenceEntry evidence : variant.getAnnotation().getTraitAssociation()) {
                        Node evNode = NodeBuilder.newNode(evidence, Node.Type.TRAIT_ASSOCIATION);
                        networkDBAdaptor.addNode(evNode, tx);

                        // Relation: variant - conservation
                        Relation vFuncRel = new Relation(-1, null, variantNode.getUid(), evNode.getUid(),
                                Relation.Type.TRAIT_ASSOCIATION);
                        networkDBAdaptor.mergeRelation(vFuncRel, tx);
                    }
                }

                // Functional scores
                if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                    for (Score score: variant.getAnnotation().getFunctionalScore()) {
                        Node funcNode = NodeBuilder.newNode(score, Node.Type.FUNCTIONAL_SCORE);
                        networkDBAdaptor.addNode(funcNode, tx);

                        // Relation: variant - conservation
                        Relation vFuncRel = new Relation(-1, null, variantNode.getUid(), funcNode.getUid(),
                                Relation.Type.FUNCTIONAL_SCORE);
                        networkDBAdaptor.mergeRelation(vFuncRel, tx);
                    }
                }
            }
        }
    }


    private List<Variant> convert(List<VariantContext> variantContexts, VariantContextToVariantConverter converter) {
        // Iterate over variant context and convert to variant
        List<Variant> variants = new ArrayList<>(variantContexts.size());
        for (VariantContext variantContext: variantContexts) {
            Variant variant = converter.convert(variantContext);
            variants.add(variant);
        }
        return variants;
    }

//    private void processVariantContexts(List<VariantContext> variantContexts, VariantContextToVariantConverter converter,
//                                        VariantParser variantParser) throws BioNetDBException {
//        // Convert to variants, parse and merge it into the final network
//        List<Variant> variants = convert(variantContexts, converter);
//
//        Network network = variantParser.parse(variants);
//
//        NetworkManager netManager = new NetworkManager(network);
//
//        Map<Long, Long> oldUidToNewUidMap = new HashMap<>();
//
//        // Check recyclable nodes to update UIDs, such as VARIANT, SAMPLE,...
//        // and update UID is necessary
//        for (Node node: netManager.getNodes()) {
//            if (node.getType() == Node.Type.SAMPLE || node.getType() == Node.Type.VARIANT) {
//                // Sample, variant nodes
//                if (idToUidMap.containsKey(node.getName())) {
//                    oldUidToNewUidMap.put(node.getUid(), idToUidMap.get(node.getName()));
//                    node.setUid(idToUidMap.get(node.getName()));
//                } else {
//                    NodeQuery query = new NodeQuery(node.getType());
//                    query.put((node.getType() == Node.Type.VARIANT ? "name" : "id"), node.getName());
//                    QueryResult<Node> queryResult = nodeQuery(query, QueryOptions.empty());
//                    if (ListUtils.isNotEmpty(queryResult.getResult())) {
//                        if (queryResult.getResult().size() != 1) {
//                            logger.error("Skipping processing: {} node {} has multiple instances", node.getType(), node.getName());
//                            continue;
//                        }
//                        idToUidMap.put(node.getName(), queryResult.getResult().get(0).getUid());
//                        oldUidToNewUidMap.put(node.getUid(), idToUidMap.get(node.getName()));
//                        node.setUid(idToUidMap.get(node.getName()));
//                    } else {
//                        idToUidMap.put(node.getName(), node.getUid());
//                    }
//                }
//            }
//        }
//
//        // Now, relations' origin and destination UIDs are updated
//        netManager.replaceRelationNodeUids(oldUidToNewUidMap);
//
//        // Load network to the database
//        networkDBAdaptor.insert(network, QueryOptions.empty());
//    }
}
