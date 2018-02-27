package org.opencb.bionetdb.core.neo4j;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
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
            // Variant node
            Node variantNode = NodeBuilder.newNode(variant);
            networkDBAdaptor.mergeNode(variantNode, "name", variantNode.getName(), tx);

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
                        networkDBAdaptor.mergeNode(sampleNode, "id", sampleNode.getId(), tx);

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
                        Relation vCallRel = new Relation(-1, variantNode.getId() + "_" + callNode.getId(), variantNode.getUid(),
                                callNode.getUid(), Relation.Type.VARIANT_CALL);
                        networkDBAdaptor.addRelation(vCallRel, tx);
                    }
                }
            }
//
//            // Variant annotation
//            if (variant.getAnnotation() != null) {
//                // Consequence types
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
//                    // internal management for Proteins
//                    Map<String, List<Node>> mapUniprotVANode = new HashMap<>();
//
//                    // consequence type nodes
//                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
//
//                    }
//                }
//
//                // Population frequencies
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
//                }
//
//                // Conservation values
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
//
//                }
//
//                // Trait associations
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
//                }
//
//                // Functional scores
//                if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
//                }
//            }
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
