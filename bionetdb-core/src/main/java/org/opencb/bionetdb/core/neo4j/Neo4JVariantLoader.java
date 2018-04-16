package org.opencb.bionetdb.core.neo4j;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.DbReferenceType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.FeatureType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.KeywordType;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.TranscriptTfbs;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.cellbase.client.rest.ClinicalVariantClient;
import org.opencb.cellbase.core.api.ClinicalDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Neo4JVariantLoader {

    private Neo4JNetworkDBAdaptor networkDBAdaptor;
    private List<String> sampleNames;

    private long uidCounter;

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

    public void loadClinivalVariants(ClinicalVariantClient clinicalClient) throws IOException {
        int batchSize = 200;
        int skip = 0;
        int numVariants;

        Query query = new Query();
        query.put(ClinicalDBAdaptor.QueryParams.SOURCE.key(), "clinvar");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.LIMIT, batchSize);

        do {
            queryOptions.put(QueryOptions.SKIP, skip);

            QueryResponse<Variant> search = clinicalClient.search(query, queryOptions);
            numVariants = search.allResultsSize();

            System.out.println("skip = " + skip + ", search = " + numVariants);
//            System.out.println("num. total results = " + search.getResponse().get(0).getNumTotalResults());

            for (QueryResult<Variant> queryResult : search.getResponse()) {
                if (ListUtils.isNotEmpty(queryResult.getResult())) {
                    loadVariants(queryResult.getResult());
                }
            }
            skip += batchSize;

            if (skip > 1000) break;

        } while (batchSize == numVariants);
    }

    public void loadVariants(List<Variant> variants) {
        // First, initialize uidCounter
        uidCounter = networkDBAdaptor.getUidCounter();

        Session session = networkDBAdaptor.getDriver().session();
        try (Transaction tx = session.beginTransaction()) {
            for (Variant variant: variants) {
                loadVariant(variant, tx);
            }
            tx.success();
        }
        session.close();

        // And finally, update uidCounter into the database (using the configuration node)
        networkDBAdaptor.setUidCounter(uidCounter);
    }

    public void loadGenes(List<Gene> genes) {
        // First, initialize uidCounter
        uidCounter = networkDBAdaptor.getUidCounter();

        Session session = networkDBAdaptor.getDriver().session();
        try (Transaction tx = session.beginTransaction()) {
            for (Gene gene : genes) {
                loadGene(gene, tx);
            }
            tx.success();
        }
        session.close();

        // And finally, update uidCounter into the database (using the configuration node)
        networkDBAdaptor.setUidCounter(uidCounter);
    }

    public void loadProteins(List<Entry> proteins) {
        // First, initialize uidCounter
        uidCounter = networkDBAdaptor.getUidCounter();

        Session session = networkDBAdaptor.getDriver().session();
        try (Transaction tx = session.beginTransaction()) {
            for (Entry protein: proteins) {
                loadProtein(protein, tx);
            }
            tx.success();
        }
        session.close();

        // And finally, update uidCounter into the database (using the configuration node)
        networkDBAdaptor.setUidCounter(uidCounter);
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void loadVariant(Variant variant, Transaction tx) {
        if (variant != null) {
//            if (StringUtils.isEmpty(variant.getId()) || ".".equals(variant.getId())) {
//                return;
//            }

            // Variant node
            Node variantNode = NodeBuilder.newNode(uidCounter, variant);
            networkDBAdaptor.mergeNode(variantNode, "name", tx);

            // Sample management
            if (ListUtils.isNotEmpty(variant.getStudies())) {
                // Only one single study is supported
                StudyEntry studyEntry = variant.getStudies().get(0);

                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                    // Create the variant call info node adding all file attributes (FILTER, QUAL, INFO fields...)
                    Node fileEntryNode = NodeBuilder.newNode(++uidCounter, variant.getStudies().get(0), variantNode);
                    networkDBAdaptor.addNode(fileEntryNode, tx);

                    for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
                        // Create the sample node
                        Node sampleNode = new Node(uidCounter, sampleNames.get(i), sampleNames.get(i), Node.Type.SAMPLE);
                        networkDBAdaptor.mergeNode(sampleNode, "id", tx);

                        // And the call node for that sample adding the format attributes
                        Node callNode = NodeBuilder.newCallNode(++uidCounter, studyEntry.getFormat(), studyEntry.getSampleData(i));
                        networkDBAdaptor.addNode(callNode, tx);

                        // Relation: sample - variant call
                        Relation sVCallRel = new Relation(++uidCounter, sampleNode.getId() + "_" + callNode.getId(), sampleNode.getUid(),
                                sampleNode.getType(), callNode.getUid(), callNode.getType(), Relation.Type.VARIANT_CALL);
                        networkDBAdaptor.mergeRelation(sVCallRel, tx);

                        // Relation: variant call - variant file info
                        Relation vFileInfoRel = new Relation(++uidCounter, callNode.getId() + "_" + fileEntryNode.getId(),
                                callNode.getUid(), callNode.getType(), fileEntryNode.getUid(), fileEntryNode.getType(),
                                Relation.Type.VARIANT_FILE_INFO);
                        networkDBAdaptor.mergeRelation(vFileInfoRel, tx);

                        // Relation: variant - variant call
                        Relation vCallRel = new Relation(++uidCounter, null, variantNode.getUid(), variantNode.getType(),
                                callNode.getUid(), callNode.getType(), Relation.Type.VARIANT_CALL);
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
                        Node ctNode = NodeBuilder.newNode(++uidCounter, ct);
                        networkDBAdaptor.addNode(ctNode, tx);

                        // Relation: variant - consequence type
                        Relation vCtRel = new Relation(++uidCounter, null, variantNode.getUid(), variantNode.getType(),
                                ctNode.getUid(), ctNode.getType(), Relation.Type.CONSEQUENCE_TYPE);
                        networkDBAdaptor.mergeRelation(vCtRel, tx);


                        // SO
                        if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                            for (SequenceOntologyTerm so: ct.getSequenceOntologyTerms()) {
                                Node soNode = new Node(uidCounter, so.getAccession(), so.getName(), Node.Type.SO);
                                networkDBAdaptor.mergeNode(soNode, "id", tx);

                                // Relation: consequence type - so
                                Relation ctSoRel = new Relation(++uidCounter, null, ctNode.getUid(), ctNode.getType(),
                                        soNode.getUid(), soNode.getType(), Relation.Type.SO);
                                networkDBAdaptor.mergeRelation(ctSoRel, tx);
                            }
                        }

                        // Protein annotation: substitution scores, keywords and features
                        if (ct.getProteinVariantAnnotation() != null) {
                            // Protein variant annotation node
                            Node annotNode = NodeBuilder.newNode(++uidCounter, ct.getProteinVariantAnnotation());
                            networkDBAdaptor.addNode(annotNode, tx);

                            // Relation: consequence type - protein variant annotation
                            Relation ctAnnotRel = new Relation(++uidCounter, null, ctNode.getUid(), ctNode.getType(),
                                    annotNode.getUid(), annotNode.getType(), Relation.Type.PROTEIN_VARIANT_ANNOTATION);
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
                                        Node proteinNode = new Node(++uidCounter, uniprotId, uniprotName, Node.Type.PROTEIN);
                                        networkDBAdaptor.addNode(proteinNode, tx);

                                        proteinNodes.add(proteinNode);
                                    }
                                } catch (BioNetDBException e) {
                                    e.printStackTrace();
                                }

                                // Link protein nodes to the protein variant annotation
                                for (Node proteinNode: proteinNodes) {
                                    // Relation: protein - protein variant annotation
                                    Relation proteinAnnotRel = new Relation(++uidCounter, null, proteinNode.getUid(),
                                            proteinNode.getType(), annotNode.getUid(), annotNode.getType(),
                                            Relation.Type.PROTEIN_VARIANT_ANNOTATION);
                                    networkDBAdaptor.mergeRelation(proteinAnnotRel, tx);
                                }
                            }

                            // Protein substitution scores
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                                for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                    Node subsNode = NodeBuilder.newNode(++uidCounter, score, Node.Type.SUBSTITUTION_SCORE);
                                    networkDBAdaptor.addNode(subsNode, tx);

                                    // Relation: protein variant annotation - substitution score
                                    Relation ctSubsRel = new Relation(++uidCounter, null, annotNode.getUid(), annotNode.getType(),
                                            subsNode.getUid(), subsNode.getType(), Relation.Type.SUBSTITUTION_SCORE);
                                    networkDBAdaptor.mergeRelation(ctSubsRel, tx);
                                }
                            }

                            // Protein keywords
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getKeywords())) {
                                for (String keyword: ct.getProteinVariantAnnotation().getKeywords()) {
                                    Node kwNode = new Node(++uidCounter, null, keyword, Node.Type.PROTEIN_KEYWORD);
                                    networkDBAdaptor.mergeNode(kwNode, "name", tx);

                                    // Relation: protein variant annotation - so
                                    Relation ctKwRel = new Relation(++uidCounter, null, annotNode.getUid(), annotNode.getType(),
                                            kwNode.getUid(), kwNode.getType(), Relation.Type.PROTEIN_KEYWORD);
                                    networkDBAdaptor.mergeRelation(ctKwRel, tx);
                                }
                            }

                            // Protein features
                            if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getFeatures())) {
                                for (ProteinFeature feature : ct.getProteinVariantAnnotation().getFeatures()) {
                                    Node featureNode = NodeBuilder.newNode(++uidCounter, feature);
                                    networkDBAdaptor.addNode(featureNode, tx);

                                    // Relation: protein variant annotation - protein feature
                                    Relation ctFeatureRel = new Relation(++uidCounter, null, annotNode.getUid(), annotNode.getType(),
                                            featureNode.getUid(), featureNode.getType(), Relation.Type.PROTEIN_FEATURE);
                                    networkDBAdaptor.mergeRelation(ctFeatureRel, tx);
                                }
                            }
                        }

                        // Gene
                        if (ct.getEnsemblGeneId() != null) {
                            Node geneNode = new Node(++uidCounter, ct.getEnsemblGeneId(), null, Node.Type.GENE);
                            networkDBAdaptor.mergeNode(geneNode, "id", tx);

                            // Relation: consequence type - gene
                            Relation ctGeneRel = new Relation(++uidCounter, null, ctNode.getUid(), ctNode.getType(),
                                    geneNode.getUid(), geneNode.getType(), Relation.Type.GENE);
                            networkDBAdaptor.mergeRelation(ctGeneRel, tx);
                        }

                        // Transcript
                        if (ct.getEnsemblTranscriptId() != null) {
                            Node transcriptNode = new Node(++uidCounter, ct.getEnsemblTranscriptId(), null, Node.Type.TRANSCRIPT);
                            networkDBAdaptor.mergeNode(transcriptNode, "id", tx);

                            // Relation: consequence type - transcript
                            Relation ctTranscriptRel = new Relation(++uidCounter, null, ctNode.getUid(), ctNode.getType(),
                                    transcriptNode.getUid(), transcriptNode.getType(), Relation.Type.TRANSCRIPT);
                            networkDBAdaptor.mergeRelation(ctTranscriptRel, tx);
                        }
                    }
                }

                // Population frequencies
                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                    for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                        Node popFreqNode = NodeBuilder.newNode(++uidCounter, popFreq);
                        networkDBAdaptor.addNode(popFreqNode, tx);

                        // Relation: variant - population frequency
                        Relation vPfRel = new Relation(++uidCounter, null, variantNode.getUid(), variantNode.getType(),
                                popFreqNode.getUid(), popFreqNode.getType(), Relation.Type.POPULATION_FREQUENCY);
                        networkDBAdaptor.mergeRelation(vPfRel, tx);
                    }
                }

                // Conservation values
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                    for (Score score: variant.getAnnotation().getConservation()) {
                        Node consNode = NodeBuilder.newNode(++uidCounter, score, Node.Type.CONSERVATION);
                        networkDBAdaptor.addNode(consNode, tx);

                        // Relation: variant - conservation
                        Relation vConsRel = new Relation(++uidCounter, null, variantNode.getUid(), variantNode.getType(),
                                consNode.getUid(), consNode.getType(), Relation.Type.CONSERVATION);
                        networkDBAdaptor.mergeRelation(vConsRel, tx);
                    }
                }

                // Trait associations
                if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                    for (EvidenceEntry evidence : variant.getAnnotation().getTraitAssociation()) {
                        Node evNode = NodeBuilder.newNode(++uidCounter, evidence, Node.Type.TRAIT_ASSOCIATION);
                        networkDBAdaptor.addNode(evNode, tx);

                        // Relation: variant - conservation
                        Relation vFuncRel = new Relation(++uidCounter, null, variantNode.getUid(), variantNode.getType(),
                                evNode.getUid(), evNode.getType(), Relation.Type.TRAIT_ASSOCIATION);
                        networkDBAdaptor.mergeRelation(vFuncRel, tx);
                    }
                }

                // Functional scores
                if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                    for (Score score: variant.getAnnotation().getFunctionalScore()) {
                        Node funcNode = NodeBuilder.newNode(++uidCounter, score, Node.Type.FUNCTIONAL_SCORE);
                        networkDBAdaptor.addNode(funcNode, tx);

                        // Relation: variant - conservation
                        Relation vFuncRel = new Relation(++uidCounter, null, variantNode.getUid(), variantNode.getType(),
                                funcNode.getUid(), funcNode.getType(), Relation.Type.FUNCTIONAL_SCORE);
                        networkDBAdaptor.mergeRelation(vFuncRel, tx);
                    }
                }
            }
        }
    }

    private void loadGene(Gene gene, Transaction tx) {
        if (gene != null) {
            // Gene node
            Node geneNode = NodeBuilder.newNode(uidCounter, gene);
            networkDBAdaptor.mergeNode(geneNode, "id", tx);

            // Transcripts
            if (ListUtils.isNotEmpty(gene.getTranscripts())) {
                for (Transcript transcript: gene.getTranscripts()) {
                    Node tNode = NodeBuilder.newNode(uidCounter, transcript);
                    networkDBAdaptor.mergeNode(tNode, "id", tx);

                    // Relation: gene - transcript
                    Relation gTRel = new Relation(++uidCounter, null, geneNode.getUid(), geneNode.getType(), tNode.getUid(),
                            tNode.getType(), Relation.Type.TRANSCRIPT);
                    networkDBAdaptor.mergeRelation(gTRel, tx);

                    // Tfbs
                    if (ListUtils.isNotEmpty(transcript.getTfbs())) {
                        for (TranscriptTfbs tfbs: transcript.getTfbs()) {
                            Node tfbsNode = NodeBuilder.newNode(++uidCounter, tfbs);
                            networkDBAdaptor.addNode(tfbsNode, tx);

                            // Relation: transcript - tfbs
                            Relation tTfbsRel = new Relation(++uidCounter, null, tNode.getUid(), tNode.getType(), tfbsNode.getUid(),
                                    tfbsNode.getType(), Relation.Type.TFBS);
                            networkDBAdaptor.mergeRelation(tTfbsRel, tx);
                        }
                    }

                    // Xrefs
                    if (ListUtils.isNotEmpty(transcript.getXrefs())) {
                        for (org.opencb.biodata.models.core.Xref xref: transcript.getXrefs()) {
                            Node xrefNode = NodeBuilder.newNode(uidCounter, xref);
                            networkDBAdaptor.mergeNode(xrefNode, "id", "dbName", tx);

                            // Relation: transcript - xref
                            Relation tXrefRel = new Relation(++uidCounter, null, tNode.getUid(), tNode.getType(), xrefNode.getUid(),
                                    xrefNode.getType(), Relation.Type.XREF);
                            networkDBAdaptor.mergeRelation(tXrefRel, tx);
                        }
                    }
                }
            }

            if (gene.getAnnotation() != null) {
                // Drug
                if (ListUtils.isNotEmpty(gene.getAnnotation().getDrugs())) {
                    for (GeneDrugInteraction drug: gene.getAnnotation().getDrugs()) {
                        Node drugNode = NodeBuilder.newNode(uidCounter, drug);
                        networkDBAdaptor.mergeNode(drugNode, "name", tx);

                        // Relation: gene - drug interaction
                        Relation gDrugRel = new Relation(++uidCounter, null, geneNode.getUid(), geneNode.getType(), drugNode.getUid(),
                                drugNode.getType(), Relation.Type.DRUG);
                        networkDBAdaptor.mergeRelation(gDrugRel, tx);
                    }
                }

                // Disease
                if (ListUtils.isNotEmpty(gene.getAnnotation().getDiseases())) {
                    for (GeneTraitAssociation disease: gene.getAnnotation().getDiseases()) {
                        Node diseaseNode = NodeBuilder.newNode(uidCounter, disease);
                        networkDBAdaptor.mergeNode(diseaseNode, "id", tx);

                        // Relation: gene - disease (trait association)
                        Relation gDiseaseRel = new Relation(++uidCounter, null, geneNode.getUid(), geneNode.getType(),
                                diseaseNode.getUid(), diseaseNode.getType(), Relation.Type.DISEASE);
                        networkDBAdaptor.mergeRelation(gDiseaseRel, tx);
                    }
                }
            }
        }
    }

    private void loadProtein(Entry protein, Transaction tx) {
        if (protein != null) {
            Node proteinNode = NodeBuilder.newNode(uidCounter, protein);
            networkDBAdaptor.mergeNode(proteinNode, "id", tx);

            // Protein keywords
            if (ListUtils.isNotEmpty(protein.getKeyword())) {
                for (KeywordType keyword: protein.getKeyword()) {
                    Node kwNode = NodeBuilder.newNode(uidCounter, keyword);
                    networkDBAdaptor.mergeNode(kwNode, "name", tx);

                    // Relation: protein variant annotation - so
                    Relation pKwRel = new Relation(++uidCounter, null, proteinNode.getUid(), proteinNode.getType(), kwNode.getUid(),
                            kwNode.getType(), Relation.Type.PROTEIN_KEYWORD);
                    networkDBAdaptor.mergeRelation(pKwRel, tx);
                }
            }

            // Protein features
            if (ListUtils.isNotEmpty(protein.getFeature())) {
                for (FeatureType feature : protein.getFeature()) {
                    Node featureNode = NodeBuilder.newNode(++uidCounter, feature);
                    networkDBAdaptor.addNode(featureNode, tx);

                    // Relation: protein variant annotation - protein feature
                    Relation pFeatureRel = new Relation(++uidCounter, null, proteinNode.getUid(), proteinNode.getType(),
                            featureNode.getUid(), featureNode.getType(), Relation.Type.PROTEIN_FEATURE);
                    networkDBAdaptor.mergeRelation(pFeatureRel, tx);
                }
            }

            // Xrefs
            if (ListUtils.isNotEmpty(protein.getDbReference())) {
                for (DbReferenceType xref : protein.getDbReference()) {
                    Node xrefNode = NodeBuilder.newNode(uidCounter, xref);
                    networkDBAdaptor.mergeNode(xrefNode, "id", "dbName", tx);

                    // Relation: protein variant annotation - protein feature
                    Relation pFeatureRel = new Relation(++uidCounter, null, proteinNode.getUid(), proteinNode.getType(),
                            xrefNode.getUid(), xrefNode.getType(), Relation.Type.XREF);
                    networkDBAdaptor.mergeRelation(pFeatureRel, tx);
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
}
