package org.opencb.bionetdb.core.neo4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.commons.collections.MapUtils;
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
import org.opencb.bionetdb.core.io.BioPaxParser;
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
import java.nio.file.Paths;
import java.util.*;

public class DemoTest {
    //    String database = "demo";
    String database = "scerevisiae";
    NetworkDBAdaptor networkDBAdaptor = null;

    //String reactomeBiopaxFilename = "/home/jtarraga/data150/neo4j/vesicle.mediated.transport.biopax3";
//    String reactomeBiopaxFilename = "/home/jtarraga/data150/neo4j/pathway.biopax";
    String reactomeBiopaxFilename = "/home/jtarraga/data150/neo4j/pathway1.biopax3";

    String variantJsonFilename = "/home/jtarraga/data150/neo4j/test2.json";

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
    public void createNetwork() throws IOException, BioNetDBException {
        // Load physical data from Reactome data: Biopax files
        loadPhysicalData();

        // Load experimental data from variant data: VCF files, (json files)
        loadExperimentalData();

        // Complete network, i.e.: search genes and check if all its transcripts are there, otherwise
        // add the remaining transcripts to the network
        completeNetwork();

        // Annotate network, it implies to annotate variants, genes and proteins
        annotateNetwork();

        // Load clinical layer
        // TODO
    }

    @Test
    public void loadPhysicalData() throws IOException, BioNetDBException {
        // Load physical data from Reactome data: Biopax files

        // ... parse Reactome data from Biopax parse
        BioPaxParser parser = new BioPaxParser("L3");
        Network network = parser.parse(Paths.get(reactomeBiopaxFilename));

        /// ...and insert into the network
        System.out.println("Inserting data...");
        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        long stopTime = System.currentTimeMillis();
        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
    }

    @Test
    public void loadExperimentalData() throws IOException, BioNetDBException {
        // Load experimental data from variant data: VCF files, (json files)

        // ...parse variants from json
        ObjectMapper mapper = new ObjectMapper();
        Variant variant = mapper.readValue(new File(variantJsonFilename), Variant.class);
        Network network = parseVariant(variant);
        //System.out.println(variant.toJson());

        // Link experimental network to physical network
        if (network.getAttributes().containsKey("_uniprot")) {
            Map<String, List<Node>> uniprotMap = (Map<String, List<Node>>) network.getAttributes().get("_uniprot");
            for (String key: uniprotMap.keySet()) {
                Query query = new Query(NetworkDBAdaptor.NetworkQueryParams.SCRIPT.key(),
                        "match (p:PROTEIN)-[xr:XREF]->(x:XREF) where x.source = \"uniprot\" and "
                        + " x.id = \"" + key + "\" return p");
                QueryResult<Node> nodes = networkDBAdaptor.getNodes(query, QueryOptions.empty());
//                System.out.println("uniprot: " + key + ", value list size = " + uniprotMap.get(key).size());
                for (Node protVANode: uniprotMap.get(key)) {
                    for (Node protein: nodes.getResult()) {
//                        System.out.println("\t" + protein.toString());

                        // ... and its relationship
                        Relationship protVARel = new Relationship(protein.getId() + protVANode.getId(),
                                protein.getId(), protein.getType().toString(), protVANode.getId(),
                                protVANode.getType().toString(), Relationship.Type.ANNOTATION);
                        network.setRelationship(protVARel);
                    }
                }
            }
        }

        // ...and insert into the network
        System.out.println("Inserting data...");
        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        long stopTime = System.currentTimeMillis();
        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
    }

    @Test
    public void completeNetwork() {
        // Complete network, i.e.: search genes and check if all its transcripts are there, otherwise
        // add the remaining transcripts to the network

    }

    @Test
    public void annotateNetwork() {
        // Annotate network, it implies to annotate variants, genes and proteins

        // ...annotate variants
        // TODO, annotateVariants();

        // ...annotate genes
        // TODO, annotateGenes();

        // ...and annotate proteins
        // TODO, annotateProteins();
    }

    @Test
    public void annotateVariants() {
        // TODO
    }

    @Test
    public void annotateGenes() throws BioNetDBException, IOException {
        // First, get all genes
        Query query = new Query(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), "GENE");
        QueryResult<Node> nodes = networkDBAdaptor.getNodes(query, null);
        for (Node node: nodes.getResult()) {
            System.out.println(node.toString());
        }
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
            Node vNode = new Node(variant.getId(), variant.toString(), Node.Type.VARIANT);
            vNode.addAttribute("chromosome", variant.getChromosome());
            vNode.addAttribute("start", variant.getStart());
            vNode.addAttribute("end", variant.getEnd());
            vNode.addAttribute("reference", variant.getReference());
            vNode.addAttribute("alternate", variant.getAlternate());
            vNode.addAttribute("strand", variant.getStrand());
            vNode.addAttribute("vtype", variant.getType().toString());
            network.setNode(vNode);

            if (variant.getAnnotation() != null) {
                // Annotation node
                Node annotNode = new Node("VariantAnnotation_" + (countId++), null, Node.Type.VARIANT_ANNOTATION);
                network.setNode(annotNode);

                // Relationship: variant - annotation
                Relationship vAnnotRel = new Relationship(vNode.getId() + annotNode.getId(), vNode.getId(), vNode.getType().toString(),
                        annotNode.getId(), annotNode.getType().toString(), Relationship.Type.ANNOTATION);
                network.setRelationship(vAnnotRel);

                if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {

                    // internal management for Proteins
                    Map<String, List<Node>> mapUniprotVANode = new HashMap<>();


                    // consequence type nodes
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
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
                        Relationship vCtRel = new Relationship(annotNode.getId() + ctNode.getId(), annotNode.getId(), annotNode.getType().toString(),
                                ctNode.getId(), ctNode.getType().toString(), Relationship.Type.CONSEQUENCE_TYPE);
                        network.setRelationship(vCtRel);

                        // Transcript nodes
                        if (ct.getEnsemblTranscriptId() != null) {
                            Node transcriptNode = new Node("Ensembl:" + ct.getEnsemblTranscriptId(), null, Node.Type.TRANSCRIPT);
                            network.setNode(transcriptNode);

                            // Relationship: consequence type - transcript
                            Relationship ctTRel = new Relationship(ctNode.getId() + transcriptNode.getId(), ctNode.getId(),
                                    ctNode.getType().toString(), transcriptNode.getId(), transcriptNode.getType().toString(),
                                    Relationship.Type.TRANSCRIPT);
                            network.setRelationship(ctTRel);

                            // Ensembl gene node
                            if (ct.getEnsemblGeneId() != null) {
                                Node eGeneNode = new Node("Ensembl:" + ct.getEnsemblGeneId(), ct.getGeneName(), Node.Type.GENE);
                                eGeneNode.addAttribute("ensemblGeneId", ct.getEnsemblGeneId());
                                //xrefEGeneNode.setSubtypes(Collections.singletonList(Node.Type.GENE));
                                network.setNode(eGeneNode);

                                // Relationship: transcript - ensembl gene
                                Relationship tEgRel = new Relationship(transcriptNode.getId() + eGeneNode.getId(),
                                        transcriptNode.getId(), transcriptNode.getType().toString(), eGeneNode.getId(),
                                        eGeneNode.getType().toString(), Relationship.Type.GENE);
                                network.setRelationship(tEgRel);
                            }

                            //
                            // Xref managements
                            //

                            // Xref ensembl transcript node
                            Xref xrefETranscriptNode = new Xref("Ensembl", "", ct.getEnsemblTranscriptId(), "");
                            network.setNode(xrefETranscriptNode);

                            // Relationship: transcript - xref ensembl transcript
                            Relationship tXEtRel = new Relationship(transcriptNode.getId() + xrefETranscriptNode.getId(),
                                    transcriptNode.getId(), transcriptNode.getType().toString(), xrefETranscriptNode.getId(),
                                    xrefETranscriptNode.getType().toString(), Relationship.Type.XREF);
                            network.setRelationship(tXEtRel);

                            // Xref ensembl gene node
                            Xref xrefEGeneNode = new Xref("Ensembl", "", ct.getEnsemblGeneId(), "");
                            network.setNode(xrefEGeneNode);

                            // Relationship: transcript - xref ensembl gene
                            Relationship tXEgRel = new Relationship(transcriptNode.getId() + xrefEGeneNode.getId(),
                                    transcriptNode.getId(), transcriptNode.getType().toString(), xrefEGeneNode.getId(),
                                    xrefEGeneNode.getType().toString(), Relationship.Type.XREF);
                            network.setRelationship(tXEgRel);

                            // Xref gene node
                            Xref xrefGeneNode = new Xref("", "", ct.getGeneName(), "");
                            network.setNode(xrefGeneNode);

                            // Relationship: transcript - xref gene
                            Relationship tXGRel = new Relationship(transcriptNode.getId() + xrefGeneNode.getId(), transcriptNode.getId(),
                                    transcriptNode.getType().toString(), xrefGeneNode.getId(), xrefGeneNode.getType().toString(),
                                    Relationship.Type.XREF);
                            network.setRelationship(tXGRel);
                        } else {
                            System.out.println("Transcript is NULL !!!");
                        }

                        // Protein variant annotation
                        if (ct.getProteinVariantAnnotation() != null) {
                            ProteinVariantAnnotation protVA = ct.getProteinVariantAnnotation();
                            // Create node
                            Node protVANode = new Node("ProteinAnnotation_" + countId++, protVA.getUniprotName(),
                                    Node.Type.PROTEIN_ANNOTATION);
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
                                    Relationship.Type.ANNOTATION);
                            network.setRelationship(ctTRel);

                            // Check for protein features
                            if (ListUtils.isNotEmpty(protVA.getFeatures())) {
                                for (ProteinFeature protFeat : protVA.getFeatures()) {
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
                                // internal management
                                if (!mapUniprotVANode.containsKey(protVA.getUniprotAccession())) {
                                    mapUniprotVANode.put(protVA.getUniprotAccession(), new ArrayList<>());
                                }
                                mapUniprotVANode.get(protVA.getUniprotAccession()).add(protVANode);

//                                // ... and create node for the protein
//                                Protein protein = new Protein();
//                                protein.setName(protVA.getUniprotAccession());
//                                protein.setXref(new Xref("uniprot", "", protVA.getUniprotAccession(), ""));
//                                network.setNode(protein);
//
//                                // ... and its relationship
//                                Relationship protVARel = new Relationship(protein.getId() + protVANode.getId(),
//                                        protein.getId(), protein.getType().toString(), protVANode.getId(),
//                                        protVANode.getType().toString(), Relationship.Type.ANNOTATION);
//                                network.setRelationship(protVARel);
                            }

                            // Check for substitution scores...
                            if (ListUtils.isNotEmpty(protVA.getSubstitutionScores())) {
                                for (Score score : protVA.getSubstitutionScores()) {
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
                            for (SequenceOntologyTerm sot : ct.getSequenceOntologyTerms()) {
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
                    if (MapUtils.isNotEmpty(mapUniprotVANode)) {
                        network.getAttributes().put("_uniprot", mapUniprotVANode);
                    }
                }

                if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                    for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                        Node popFreqNode = new Node("PopulationFrequency_" + (countId++), null, Node.Type.POPULATION_FREQUENCY);
                        popFreqNode.addAttribute("study", popFreq.getStudy());
                        popFreqNode.addAttribute("population", popFreq.getPopulation());
                        popFreqNode.addAttribute("refAlleleFreq", popFreq.getRefAlleleFreq());
                        popFreqNode.addAttribute("altAlleleFreq", popFreq.getAltAlleleFreq());
                        network.setNode(popFreqNode);

                        // Relationship: variant - population frequency
                        Relationship vPfRel = new Relationship(annotNode.getId() + popFreqNode.getId(), annotNode.getId(), annotNode.getType().toString(),
                                popFreqNode.getId(), popFreqNode.getType().toString(), Relationship.Type.POPULATION_FREQUENCY);
                        network.setRelationship(vPfRel);

                    }
                }

                // Conservation
                if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                    for (Score score : variant.getAnnotation().getConservation()) {
                        Node conservNode = new Node("Conservation_" + (countId++), null, Node.Type.CONSERVATION);
                        conservNode.addAttribute("score", score.getScore());
                        conservNode.addAttribute("source", score.getSource());
                        conservNode.addAttribute("description", score.getDescription());
                        network.setNode(conservNode);

                        // Relationship: variant - conservation
                        Relationship vConservRel = new Relationship(annotNode.getId() + conservNode.getId(), annotNode.getId(),
                                annotNode.getType().toString(), conservNode.getId(), conservNode.getType().toString(),
                                Relationship.Type.CONSERVATION);
                        network.setRelationship(vConservRel);
                    }
                }

                // Trait association
                if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                    for (EvidenceEntry evidence : variant.getAnnotation().getTraitAssociation()) {
                        Node evNode = new Node("TraitAssociation_" + (countId++), null, Node.Type.TRAIT_ASSOCIATION);
                        if (evidence.getSource() != null && evidence.getSource().getName() != null) {
                            evNode.addAttribute("source", evidence.getSource().getName());
                        }
                        evNode.addAttribute("url", evidence.getUrl());
                        if (ListUtils.isNotEmpty(evidence.getHeritableTraits())) {
                            StringBuilder her = new StringBuilder();
                            for (HeritableTrait heritableTrait : evidence.getHeritableTraits()) {
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
                            for (AlleleOrigin alleleOrigin : evidence.getAlleleOrigin()) {
                                if (alleleOri.length() > 0 && alleleOrigin.name() != null) {
                                    alleleOri.append(",");
                                }
                                alleleOri.append(alleleOrigin.name());
                            }
                            evNode.addAttribute("alleleOrigin", alleleOri.toString());
                        }
                        network.setNode(evNode);

                        // Relationship: variant - conservation
                        Relationship vFuncRel = new Relationship(annotNode.getId() + evNode.getId(), annotNode.getId(),
                                annotNode.getType().toString(), evNode.getId(), evNode.getType().toString(),
                                Relationship.Type.TRAIT_ASSOCIATION);
                        network.setRelationship(vFuncRel);

                    }
                }


                // Functional score
                if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                    for (Score score : variant.getAnnotation().getFunctionalScore()) {
                        Node funcNode = new Node("FunctionalScore_" + (countId++), null, Node.Type.FUNCTIONAL_SCORE);
                        funcNode.addAttribute("score", score.getScore());
                        funcNode.addAttribute("source", score.getSource());
                        network.setNode(funcNode);

                        // Relationship: variant - conservation
                        Relationship vTraitRel = new Relationship(annotNode.getId() + funcNode.getId(), annotNode.getId(),
                                annotNode.getType().toString(), funcNode.getId(), funcNode.getType().toString(),
                                Relationship.Type.FUNCTIONAL_SCORE);
                        network.setRelationship(vTraitRel);

                    }
                }
            }
        }
        return network;
    }

//        ObjectMapper mapper = new ObjectMapper();
//        mapper.writeValue(new File("/tmp/vesicle.mediated.transport.network.json"), network);
//        Variant variant = mapper.readValue(new File("/home/jtarraga/data150/neo4j/test2.json"), Variant.class);

}
