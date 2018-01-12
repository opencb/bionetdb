package org.opencb.bionetdb.core.neo4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.*;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DemoTest {
    //    String database = "demo";
    String database = "scerevisiae";
    NetworkDBAdaptor networkDBAdaptor = null;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void initialize () {
        // Remove existing database
//        try {
//            deleteRecursively(new File(database));
//        } catch ( IOException e ) {
//            throw new RuntimeException( e );
//        }
//        // Create again the path to the database
//        new File(database).mkdirs();
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig: bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration, true);
        } catch (BioNetDBException | IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void close() throws Exception {
        networkDBAdaptor.close();
    }

    @Test
    public void createPhysicalNetwork() {
        System.out.println("Creating physical network: reactome from biopax file");
    }

    @Test
    public void createExperimentalNetwork() throws IOException, BioNetDBException {
        ObjectMapper mapper = new ObjectMapper();
        Variant variant = mapper.readValue(new File("~/data150/neo4j/test2.json"), Variant.class);
        Network network = parseVariant(variant);
        //System.out.println(variant.toJson());

        System.out.println("Inserting data...");
        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        long stopTime = System.currentTimeMillis();
        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
    }

    @Test
    public void annotateProteins() throws BioNetDBException, IOException {
        Query query = new Query();

        // First, get all proteins
        query.put(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), "PROTEIN");
        QueryResult<Node> nodes = networkDBAdaptor.getNodes(query, null);
        for (Node node: nodes.getResult()) {
            System.out.println(node.toString());
        }

        // Get proteins annotations from Cellbase...
        // ... create the Cellbase protein client
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
        ProteinClient proteinClient = cellBaseClient.getProteinClient();

        // ... prepare list of protein id/names
        List<String> proteinIds = new ArrayList<>();
        for (Node node: nodes.getResult()) {
            if (node.getName() != null) {
                proteinIds.add(node.getName());
            }
        }

        // ... finally, call Cellbase service
        QueryResponse<Entry> entryQueryResponse = proteinClient.get(proteinIds, new QueryOptions(QueryOptions.EXCLUDE, "reference,organism," +
                "comment,evidence,sequence"));
        for (Entry entry: entryQueryResponse.getResponse().get(0).getResult()) {
            System.out.println(entry.toString());
        }

        // Add annotations to the network

    }

    public Network parseVariant(Variant variant) {
        int countId = 0;
        Network network = new Network();
        if (variant != null) {
            // main node
            Node main = new Node(variant.getId(), variant.toString(), Node.Type.VARIANT);
            main.addAttribute("chromosome", variant.getChromosome());
            main.addAttribute("start", variant.getStart());
            main.addAttribute("end", variant.getEnd());
            main.addAttribute("reference", variant.getReference());
            main.addAttribute("alternate", variant.getAlternate());
            main.addAttribute("strand", variant.getStrand());
            main.addAttribute("vtype", variant.getType().toString());
            network.setNode(main);

            if (variant.getAnnotation() != null) {
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                    // consequence type nodes
                    for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
                        if (ct.getBiotype() == null) {
                            continue;
                        }
                        Node ctNode = new Node("ConsequenceType_" + (countId++), null, Node.Type.CONSEQUENCE_TYPE);
                        ctNode.addAttribute("biotype", ct.getBiotype());
                        if (ListUtils.isNotEmpty(ct.getTranscriptAnnotationFlags())) {
                            ctNode.addAttribute("transcriptAnnotationFlags", String.join(",", ct.getTranscriptAnnotationFlags()));
                        }
                        ctNode.addAttribute("cdnaPosition", ct.getCdnaPosition());
                        ctNode.addAttribute("cdsPosition", ct.getCdsPosition());
                        ctNode.addAttribute("codon", ct.getCodon());
                        network.setNode(ctNode);

                        // Relationship: variant - consequence type
                        Relationship vCtRel = new Relationship(main.getId() + ctNode.getId(), main.getId(), main.getType().toString(),
                                ctNode.getId(), ctNode.getType().toString(), Relationship.Type.CONSEQUENCE_TYPE);
                        network.setRelationship(vCtRel);

                        // Transcript nodes
                        if (ct.getEnsemblTranscriptId() != null) {
                            Node transcriptNode = new Node(ct.getEnsemblTranscriptId(), null, Node.Type.TRANSCRIPT);
                            network.setNode(transcriptNode);

                            // Ensembl gene node
                            if (ct.getEnsemblGeneId() != null && ct.getGeneName() != null) {
                                Node eGeneNode = new Node("Gene_" + (countId++), ct.getGeneName(), Node.Type.GENE);
                                eGeneNode.addAttribute("ensemblGeneId", ct.getEnsemblGeneId());
                                //xrefEGeneNode.setSubtypes(Collections.singletonList(Node.Type.GENE));
                                network.setNode(eGeneNode);

                                // Relationship: transcript - ensembl gene
                                Relationship tEgRel = new Relationship(transcriptNode.getId() + eGeneNode.getId(),
                                        transcriptNode.getId(), transcriptNode.getType().toString(), eGeneNode.getId(),
                                        eGeneNode.getType().toString(), Relationship.Type.GENE);
                                network.setRelationship(tEgRel);
                            }

                            // Xref ensembl gene node
                            Node xrefEGeneNode = new Node(ct.getEnsemblGeneId(), null, Node.Type.XREF);
                            //xrefEGeneNode.setSubtypes(Collections.singletonList(Node.Type.GENE));
                            network.setNode(xrefEGeneNode);

                            // Relationship: transcript - xref ensembl gene
                            Relationship tXEgRel = new Relationship(transcriptNode.getId() + xrefEGeneNode.getId(),
                                    transcriptNode.getId(), transcriptNode.getType().toString(), xrefEGeneNode.getId(),
                                    xrefEGeneNode.getType().toString(), Relationship.Type.XREF);
                            network.setRelationship(tXEgRel);

                            // Xref gene node
                            Node xrefGeneNode = new Node(ct.getGeneName(), null, Node.Type.XREF);
                            network.setNode(xrefGeneNode);

                            // Relationship: transcript - xref gene
                            Relationship tXGRel = new Relationship(transcriptNode.getId() + xrefGeneNode.getId(), transcriptNode.getId(),
                                    transcriptNode.getType().toString(), xrefGeneNode.getId(), xrefGeneNode.getType().toString(),
                                    Relationship.Type.XREF);
                            network.setRelationship(tXGRel);

                            // Relationship: consequence type - transcript
                            Relationship ctTRel = new Relationship(ctNode.getId() + transcriptNode.getId(), ctNode.getId(),
                                    ctNode.getType().toString(), transcriptNode.getId(), transcriptNode.getType().toString(),
                                    Relationship.Type.TRANSCRIPT);
                            network.setRelationship(ctTRel);
                        } else {
                            System.out.println("Transcript is NULL !!!");
                        }

                        // Protein variant annotation
                        if (ct.getProteinVariantAnnotation() != null) {
                            ProteinVariantAnnotation protVA = ct.getProteinVariantAnnotation();
                            // Create node
                            Node protVANode = new Node("ProteinVarAnnotation_" + countId++, protVA.getUniprotName(),
                                    Node.Type.PROTEIN_VARIANT_ANNOTATION);
                            protVANode.addAttribute("uniprotAccession", protVA.getUniprotAccession());
                            protVANode.addAttribute("uniprotName", protVA.getUniprotName());
                            protVANode.addAttribute("uniprotVariantId", protVA.getUniprotVariantId());
                            protVANode.addAttribute("functionalDescription", protVA.getFunctionalDescription());
                            if (ListUtils.isNotEmpty(protVA.getKeywords())) {
                                protVANode.addAttribute("keywords", String.join(",", protVA.getKeywords()));
                            }
                            protVANode.addAttribute("reference", protVA.getReference());
                            protVANode.addAttribute("alternate", protVA.getAlternate());
                            network.setNode(protVANode);

                            // And create relationship consequence type -> protein variation annotation
                            Relationship ctTRel = new Relationship(ctNode.getId() + protVANode.getId(), ctNode.getId(),
                                    ctNode.getType().toString(), protVANode.getId(), protVANode.getType().toString(),
                                    Relationship.Type.PROTEIN_VARIANT_ANNOTATION);
                            network.setRelationship(ctTRel);

                            // Check for protein features
                            if (ListUtils.isNotEmpty(protVA.getFeatures())) {
                                for (ProteinFeature protFeat: protVA.getFeatures()) {
                                    // ... and create node for each protein feature
                                    Node protFeatNode = new Node();
                                    protFeatNode.setId(protFeat.getId() == null ? "ProteinFeature_" + (countId++)
                                            : protFeat.getId());
                                    protFeatNode.setType(Node.Type.PROTEIN_FEATURE);
                                    protFeatNode.addAttribute("ftype", protFeat.getType());
                                    protFeatNode.addAttribute("description", protFeat.getDescription());
                                    protFeatNode.addAttribute("start", protFeat.getStart());
                                    protFeatNode.addAttribute("end", protFeat.getEnd());
                                    network.setNode(protFeatNode);

                                    // ... and its relationship
                                    Relationship protVAFeatRel = new Relationship(protVANode.getId()
                                            + protFeatNode.getId(), protVANode.getId(), protVANode.getType().toString(),
                                            protFeatNode.getId(), protFeatNode.getType().toString(),
                                            Relationship.Type.PROTEIN_FEATURE);
                                    network.setRelationship(protVAFeatRel);
                                }
                            }

                            // Check for protein...
                            if (protVA.getUniprotAccession() != null) {
                                // ... and create node for the protein
                                Protein protein = new Protein();
                                protein.setName(protVA.getUniprotAccession());
                                protein.setXref(new Xref("uniprot", "", protVA.getUniprotAccession(), ""));
                                network.setNode(protein);

                                // ... and its relationship
                                Relationship protVARel = new Relationship(protein.getId() + protVANode.getId(),
                                        protein.getId(), protein.getType().toString(), protVANode.getId(),
                                        protVANode.getType().toString(), Relationship.Type.PROTEIN);
                                network.setRelationship(protVARel);
                            }

                            // Check for substitution scores...
                            if (ListUtils.isNotEmpty(protVA.getSubstitutionScores())) {
                                for (Score score: protVA.getSubstitutionScores()) {
                                    // ... and create node for each substitution score
                                    Node substNode = new Node("SubstitutionScore_" + (countId++), null, Node.Type.SUBST_SCORE);
                                    substNode.addAttribute("score", score.getScore());
                                    substNode.addAttribute("source", score.getSource());
                                    substNode.addAttribute("description", score.getDescription());
                                    network.setNode(substNode);

                                    // ... and its relationship
                                    Relationship protVASubstRel = new Relationship(protVANode.getId() + substNode.getId(), protVANode.getId(),
                                            protVANode.getType().toString(), substNode.getId(), substNode.getType().toString(),
                                            Relationship.Type.SUBST_SCORE);
                                    network.setRelationship(protVASubstRel);
                                }
                            }

                        }

                        // Sequence Ontology terms
                        if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                            // Sequence Ontology term nodes
                            for (SequenceOntologyTerm sot: ct.getSequenceOntologyTerms()) {
                                Node soNode = new Node(sot.getAccession(), sot.getName(), Node.Type.SO);
                                soNode.addAttribute("accession", sot.getAccession());
                                network.setNode(soNode);

                                // Relationship: consequence type - so
                                Relationship ctSoRel = new Relationship(ctNode.getId() + soNode.getId(), ctNode.getId(),
                                        ctNode.getType().toString(), soNode.getId(), soNode.getType().toString(), Relationship.Type.SO);
                                network.setRelationship(ctSoRel);
                            }
                        }
                    }
                }

                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                    for (PopulationFrequency popFreq: variant.getAnnotation().getPopulationFrequencies()) {
                        Node popFreqNode = new Node("PopulationFrequency_" + (countId++), null, Node.Type.POPULATION_FREQUENCY);
                        popFreqNode.addAttribute("study", popFreq.getStudy());
                        popFreqNode.addAttribute("population", popFreq.getPopulation());
                        popFreqNode.addAttribute("refAlleleFreq", popFreq.getRefAlleleFreq());
                        popFreqNode.addAttribute("altAlleleFreq", popFreq.getAltAlleleFreq());
                        network.setNode(popFreqNode);

                        // Relationship: variant - population frequency
                        Relationship vPfRel = new Relationship(main.getId() + popFreqNode.getId(), main.getId(), main.getType().toString(),
                                popFreqNode.getId(), popFreqNode.getType().toString(), Relationship.Type.POPULATION_FREQUENCY);
                        network.setRelationship(vPfRel);

                    }
                }

                // Conservation
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                    for (Score score: variant.getAnnotation().getConservation()) {
                        Node conservNode = new Node("Conservation_" + (countId++), null, Node.Type.CONSERVATION);
                        conservNode.addAttribute("score", score.getScore());
                        conservNode.addAttribute("source", score.getSource());
                        conservNode.addAttribute("description", score.getDescription());
                        network.setNode(conservNode);

                        // Relationship: variant - conservation
                        Relationship vConservRel = new Relationship(main.getId() + conservNode.getId(), main.getId(),
                                main.getType().toString(), conservNode.getId(), conservNode.getType().toString(),
                                Relationship.Type.CONSERVATION);
                        network.setRelationship(vConservRel);
                    }
                }
            }

            // Trait association
            if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                for (EvidenceEntry evidence: variant.getAnnotation().getTraitAssociation()) {
                    Node evNode = new Node("TraitAssociation_" + (countId++), null, Node.Type.TRAIT_ASSOCIATION);
                    if (evidence.getSource() != null && evidence.getSource().getName() != null) {
                        evNode.addAttribute("source", evidence.getSource().getName());
                    }
                    evNode.addAttribute("url", evidence.getUrl());
                    if (ListUtils.isNotEmpty(evidence.getHeritableTraits())) {
                        StringBuilder her = new StringBuilder();
                        for (HeritableTrait heritableTrait: evidence.getHeritableTraits()) {
                            if (her.length() > 0) {
                                her.append(",");
                            }
                            her.append(heritableTrait.getTrait());
                        }
                        evNode.addAttribute("heritableTraits", her.toString());
                    }
                    if (evidence.getSource() != null && evidence.getSource().getName() != null) {
                        evNode.addAttribute("source", evidence.getSource().getName());
                    }
                    if (ListUtils.isNotEmpty(evidence.getAlleleOrigin())) {
                        StringBuilder alleleOri = new StringBuilder();
                        for (AlleleOrigin alleleOrigin: evidence.getAlleleOrigin()) {
                            if (alleleOri.length() > 0 && alleleOrigin.name() != null) {
                                alleleOri.append(",");
                            }
                            alleleOri.append(alleleOrigin.name());
                        }
                        evNode.addAttribute("alleleOrigin", alleleOri.toString());
                    }
                    network.setNode(evNode);

                    // Relationship: variant - conservation
                    Relationship vFuncRel = new Relationship(main.getId() + evNode.getId(), main.getId(),
                            main.getType().toString(), evNode.getId(), evNode.getType().toString(),
                            Relationship.Type.TRAIT_ASSOCIATION);
                    network.setRelationship(vFuncRel);

                }
            }


            // Functional score
            if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    Node funcNode = new Node("FunctionalScore_" + (countId++), null, Node.Type.FUNCTIONAL_SCORE);
                    funcNode.addAttribute("score", score.getScore());
                    funcNode.addAttribute("source", score.getSource());
                    network.setNode(funcNode);

                    // Relationship: variant - conservation
                    Relationship vTraitRel = new Relationship(main.getId() + funcNode.getId(), main.getId(),
                            main.getType().toString(), funcNode.getId(), funcNode.getType().toString(),
                            Relationship.Type.FUNCTIONAL_SCORE);
                    network.setRelationship(vTraitRel);

                }
            }
        }
        return network;
    }
}
