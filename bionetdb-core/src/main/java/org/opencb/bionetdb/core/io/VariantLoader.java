package org.opencb.bionetdb.core.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.network.Node;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class VariantLoader {

    private Neo4JNetworkDBAdaptor networkDBAdaptor;
    private List<String> sampleNames;

    private static final int VARIANT_BATCH_SIZE = 10000;

    public VariantLoader(Neo4JNetworkDBAdaptor networkDBAdaptor) {
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
            // Main node
            Node vNode = createVariantNode(variant);
            StatementResult vNeoNode = networkDBAdaptor.addNode(vNode, "name", vNode.getName(), tx);

//            //vStmResult.peek().get
//            // Sample management
//            if (ListUtils.isNotEmpty(variant.getStudies())) {
//                // Only one single study is supported
//                StudyEntry studyEntry = variant.getStudies().get(0);
//
//                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
//                }
//            }
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

    private Node createVariantNode(Variant variant) {
        Node vNode = new Node(-1, variant.getId(), variant.toString(), Node.Type.VARIANT);
        vNode.addAttribute("chromosome", variant.getChromosome());
        vNode.addAttribute("start", variant.getStart());
        vNode.addAttribute("end", variant.getEnd());
        vNode.addAttribute("reference", variant.getReference());
        vNode.addAttribute("alternate", variant.getAlternate());
        vNode.addAttribute("strand", variant.getStrand());
        vNode.addAttribute("type", variant.getType().toString());
        return vNode;
    }
}
