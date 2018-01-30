package org.opencb.bionetdb.core.neo4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.DbReferenceType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.KeywordType;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.avro.Expression;
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
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.cellbase.client.rest.ProteinClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class DemoTest {
    //    String database = "demo";
    String database = "scerevisiae";
    NetworkDBAdaptor networkDBAdaptor = null;
    CellBaseClient cellBaseClient = null;

    //String reactomeBiopaxFilename = "~/data150/neo4j/vesicle.mediated.transport.biopax3";
//    String reactomeBiopaxFilename = "~/data150/neo4j/pathway.biopax";
    String reactomeBiopaxFilename = "~/data150/neo4j/pathway1.biopax3";

    String variantJsonFilename = "~/data150/neo4j/test2.json";

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

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
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

        // ...link experimental network to physical network
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
    public void completeNetwork() throws BioNetDBException, IOException {
        // Get transcripts for each gene in the network. Transcript are gotten by Cellbase.
        // The gene and transcript are related and added to the network.

        // In addition, if the transcript is related to a protein, it is searched in the network
        // and then the relationship transcript - protein is added to the network.

        Network network = new Network();
        GeneClient geneClient = cellBaseClient.getGeneClient();

        // Complete network, i.e.: search genes and check if all its transcripts are there, otherwise
        // add the remaining transcripts to the network
        Query query = new Query(NetworkDBAdaptor.NetworkQueryParams.SCRIPT.key(), "match (g:GENE) return g");
        QueryResult<Node> nodes = networkDBAdaptor.getNodes(query, QueryOptions.empty());
        List<String> geneIds = new ArrayList<>();
        for (Node geneNode: nodes.getResult()) {
            System.out.println("\t" + geneNode.toString());

            // ...call Cellbase service
            QueryResponse<Transcript> entryQueryResponse = geneClient.getTranscript(geneNode.getId().split(":")[1],
                    new QueryOptions(QueryOptions.EXCLUDE, "exons,cDnaSequence"));
            //(geneIds, new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,"
//                    + "transcripts.cDnaSequence"));
            for (Transcript transcript: entryQueryResponse.getResponse().get(0).getResult()) {
                // ...create transcript node for the network
                Node transcriptNode = new Node("Ensembl:" + transcript.getId(), null, Node.Type.TRANSCRIPT);
                network.setNode(transcriptNode);

                // ...and add relationship transcript to gene
                Relationship tEgRel = new Relationship(transcriptNode.getId() + geneNode.getId(),
                        transcriptNode.getId(), transcriptNode.getType().toString(), geneNode.getId(),
                        geneNode.getType().toString(), Relationship.Type.GENE);
                network.setRelationship(tEgRel);

                for (org.opencb.biodata.models.core.Xref xref: transcript.getXrefs()) {

                    // ...add xrefs nodes and relationships to transcript
                    Xref xrefNode = new Xref(xref.getDbName(), "", xref.getId(), "");
                    network.setNode(xrefNode);

                    Relationship tXEgRel = new Relationship(transcriptNode.getId() + xrefNode.getId(),
                            transcriptNode.getId(), transcriptNode.getType().toString(), xrefNode.getId(),
                            xrefNode.getType().toString(), Relationship.Type.XREF);
                    network.setRelationship(tXEgRel);

                    // check to link to protein
                    if (xref.getDbName().equals("uniprotkb_acc")) {
                        System.out.println(xref.getDbName() + ", " + xref.getDbDisplayName() + ", " + xref.getId());

                        query = new Query(NetworkDBAdaptor.NetworkQueryParams.SCRIPT.key(),
                                "match (p:PROTEIN)-[xr:XREF]->(x:XREF) where x.source = \"uniprot\" and "
                                        + " x.id = \"" + xref.getId() + "\" return p");
                        nodes = networkDBAdaptor.getNodes(query, QueryOptions.empty());
                        for (Node proteinNode: nodes.getResult()) {
//                        System.out.println("\t" + protein.toString());

                            // ... and its relationship: transcript - protein
                            Relationship transcriptProtRel = new Relationship(transcriptNode.getId() + proteinNode.getId(),
                                    transcriptNode.getId(), transcriptNode.getType().toString(), proteinNode.getId(),
                                    proteinNode.getType().toString(), Relationship.Type.PROTEIN);
                            network.setRelationship(transcriptProtRel);
                        }
                    }
                }
                //System.out.println(transcript.toString());
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
    public void annotateNetwork() throws IOException, BioNetDBException {
        // Annotate network, it implies to annotate variants, genes and proteins

        // ...annotate variants
        // TODO, annotateVariants();

        // ...annotate genes
        annotateGenes();

        // ...and annotate proteins
        // TODO, annotateProteins();
    }

    @Test
    public void annotateVariants() {
        // TODO
    }

    @Test
    public void annotateGenes() throws BioNetDBException, IOException {
        Network network = new Network();
        GeneClient geneClient = cellBaseClient.getGeneClient();

        // First, get all genes
        Query query = new Query(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key(), "GENE");
        QueryResult<Node> nodes = networkDBAdaptor.getNodes(query, null);
        for (Node node: nodes.getResult()) {
            System.out.println(node.toString());
        }

        // ... prepare list of gene id/names required to call CellBase
        List<String> geneIds = new ArrayList<>();
        Map<String, Node> geneMap = new HashMap<>();
        for (Node node: nodes.getResult()) {
            String geneId = node.getId().split(":")[1];
            geneIds.add(geneId);
            geneMap.put(geneId, node);
        }

        // ... finally, call Cellbase service
        QueryResponse<Gene> geneQueryResponse = geneClient.get(geneIds, new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,"
                + "transcripts.cDnaSequence"));
        for (QueryResult<Gene> queryResult: geneQueryResponse.getResponse()) {
            Gene gene = queryResult.getResult().get(0);
//            System.out.println(gene.toString());

            // ...complete gene
            Node geneNode = geneMap.get(gene.getId());
            ObjectMap update = new ObjectMap();
            update.put("chromosome", gene.getChromosome());
            update.put("start", gene.getStart());
            update.put("end", gene.getEnd());
            update.put("strand", gene.getStrand());
            update.put("biotype", gene.getBiotype());
            update.put("status", gene.getStatus());
            geneNode.addAttribute("_update", update);
            network.setNode(geneNode);

            if (gene.getAnnotation() != null) {
                // first, create network node...
                Node geneAnnotNode = new Node("Annotation:Ensembl:" + gene.getId(), null, Node.Type.GENE_ANNOTATION);
                network.setNode(geneAnnotNode);

                // ... and its relationship: gene - gene annotation
                Relationship geneAnnotRel = new Relationship(geneNode.getId() + geneAnnotNode.getId(),
                        geneNode.getId(), geneNode.getType().toString(), geneAnnotNode.getId(),
                        geneAnnotNode.getType().toString(), Relationship.Type.ANNOTATION);
                network.setRelationship(geneAnnotRel);

                Random rnd = new Random(System.currentTimeMillis());
                for (Expression expression: gene.getAnnotation().getExpression()) {
                    // first, create network node...
                    String exprId = "Expression_" + expression.getFactorValue() + "_" + expression.getExperimentId()
                            + "_" + expression.getPvalue() + rnd.nextInt();
                    Node exprNode = new Node(exprId, null, Node.Type.EXPRESION);
                    exprNode.addAttribute("geneName", expression.getGeneName());
                    exprNode.addAttribute("transcriptId", expression.getTranscriptId());
                    exprNode.addAttribute("experimentalFactor", expression.getExperimentalFactor());
                    exprNode.addAttribute("factorValue", expression.getFactorValue());
                    exprNode.addAttribute("experimentId", expression.getExperimentId());
                    exprNode.addAttribute("technologyPlatform", expression.getTechnologyPlatform());
                    exprNode.addAttribute("expression", expression.getExpression().name());
                    exprNode.addAttribute("pvalue", expression.getPvalue());
                    network.setNode(exprNode);

                    // ... and its relationship: gene - ge3ne annotation
                    Relationship annotExprRel = new Relationship(geneAnnotNode.getId() + exprNode.getId(),
                            geneAnnotNode.getId(), geneAnnotNode.getType().toString(), exprNode.getId(),
                            exprNode.getType().toString(), Relationship.Type.EXPRESSION);
                    network.setRelationship(annotExprRel);
                }

                for (GeneTraitAssociation geneTraitAssociation: gene.getAnnotation().getDiseases()) {
                    // first, create network node...
                    Node traitNode = new Node(geneTraitAssociation.getId(), geneTraitAssociation.getName(), Node.Type.DISEASE);
                    traitNode.addAttribute("hpo", geneTraitAssociation.getHpo());
                    traitNode.addAttribute("score", geneTraitAssociation.getScore());
                    traitNode.addAttribute("source", geneTraitAssociation.getSource());
                    traitNode.addAttribute("numberOfPubmeds", geneTraitAssociation.getNumberOfPubmeds());
                    traitNode.addAttribute("types", StringUtils.join(geneTraitAssociation.getAssociationTypes(), ","));
                    traitNode.addAttribute("sources", StringUtils.join(geneTraitAssociation.getSources(), ","));
                    network.setNode(traitNode);

                    // ... and its relationship: gene - ge3ne annotation
                    Relationship annotTraitRel = new Relationship(geneAnnotNode.getId() + traitNode.getId(),
                            geneAnnotNode.getId(), geneAnnotNode.getType().toString(), traitNode.getId(),
                            traitNode.getType().toString(), Relationship.Type.DISEASE);
                    network.setRelationship(annotTraitRel);
                }

                for (GeneDrugInteraction geneDrugInteraction: gene.getAnnotation().getDrugs()) {
                    // first, create network node...
                    Node drugNode = new Node(geneDrugInteraction.getDrugName(), null, Node.Type.DRUG);
                    drugNode.addAttribute("geneName", geneDrugInteraction.getGeneName());
                    drugNode.addAttribute("studyType", geneDrugInteraction.getStudyType());
                    drugNode.addAttribute("source", geneDrugInteraction.getSource());
                    network.setNode(drugNode);

                    // ... and its relationship: gene - ge3ne annotation
                    Relationship annotDrugRel = new Relationship(geneAnnotNode.getId() + drugNode.getId(),
                            geneAnnotNode.getId(), geneAnnotNode.getType().toString(), drugNode.getId(),
                            drugNode.getType().toString(), Relationship.Type.DRUG);
                    network.setRelationship(annotDrugRel);
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
    public void annotateProteins() throws BioNetDBException, IOException {
        Network networkToUpdate = new Network();

        // Get proteins annotations from Cellbase...
        // ... create the Cellbase protein client
        ProteinClient proteinClient = cellBaseClient.getProteinClient();

        // First, get all proteins from the network
        Query query = new Query();
        String cypher = "MATCH path=(p:PROTEIN)-[xr:XREF]->(x:XREF) WHERE x.source = \"uniprot\" return path";
        query.put(NetworkDBAdaptor.NetworkQueryParams.SCRIPT.key(), cypher);
        QueryResult<Network> networkResult = networkDBAdaptor.getNetwork(query, null);

        if (ListUtils.isEmpty(networkResult.getResult())) {
            System.out.println("Network not found!!");
            return;
        }

        Network network = networkResult.getResult().get(0);

        // ... prepare list of protein id/names from xref/protein nodes
        List<String> proteinIds = new ArrayList<>();
        for (Node node: network.getNodes()) {
            if (node.getType() == Node.Type.XREF) {
                proteinIds.add(node.getId());
            }
        }

        // ... finally, call Cellbase service
        Map<String, Entry> proteinMap = new HashMap<>();
        QueryResponse<Entry> entryQueryResponse = proteinClient.get(proteinIds, new QueryOptions(QueryOptions.EXCLUDE,
                "reference,organism,comment,evidence,sequence"));
        for (QueryResult<Entry> queryResult: entryQueryResponse.getResponse()) {
            proteinMap.put(queryResult.getId(), queryResult.getResult().get(0));
        }


        for (Relationship relationship: network.getRelationships()) {
            String xrefNodeId = relationship.getDestId();
            Entry entry = proteinMap.get(relationship.getDestId());
            Protein proteinNode = new Protein();
            proteinNode.setId(relationship.getOriginId());
            networkToUpdate.setNode(proteinNode);

            // Add XREF nodes for each protein
            for (DbReferenceType dbReference: entry.getDbReference()) {
                // ... create the Xref node
                Xref xrefNode = new Xref(dbReference.getType(), "", dbReference.getId(), null);
                networkToUpdate.setNode(xrefNode);

                // ... and create relationship protein -> protein variation annotation
                Relationship pXRel = new Relationship(proteinNode.getId() + xrefNode.getId(), proteinNode.getId(),
                        proteinNode.getType().toString(), xrefNode.getId(), xrefNode.getType().toString(),
                        Relationship.Type.XREF);
                networkToUpdate.setRelationship(pXRel);
            }

            // Add PROTEIN_ANNOTATION node for each protein
            // ... create the Xref node
            String protAnnotNodeId = "ProteinAnnotation_uniprot:" + relationship.getDestId();
            Node protAnnotNode = new Node(protAnnotNodeId, null, Node.Type.PROTEIN_ANNOTATION);
            ObjectMap update = new ObjectMap();
            List<String> keywords = new ArrayList<>();
            for (KeywordType keyword: entry.getKeyword()) {
                keywords.add(keyword.getValue());
            }
            update.put("keywords", String.join(",", keywords));
            protAnnotNode.addAttribute("_update", update);

            network.setNode(protAnnotNode);

            // ...and create relationship consequence type -> protein variation annotation
            Relationship protAnnotRel = new Relationship(relationship.getOriginId() + protAnnotNode.getId(),
                    relationship.getOriginId(), Node.Type.PROTEIN.name(), protAnnotNode.getId(),
                    protAnnotNode.getType().toString(), Relationship.Type.ANNOTATION);
            networkToUpdate.setRelationship(protAnnotRel);
        }

        // Add annotations to the network
        System.out.println("Inserting data...");
        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(networkToUpdate, null);
        long stopTime = System.currentTimeMillis();
        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
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
                            String protVANodeId;
                            if (protVA.getUniprotAccession() != null) {
                                protVANodeId = "ProteinAnnotation_uniprot:" + protVA.getUniprotAccession();
                            } else {
                                protVANodeId = "ProteinAnnotation_" + countId++;
                            }
                            Node protVANode = new Node(protVANodeId, protVA.getUniprotName(), Node.Type.PROTEIN_ANNOTATION);
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
//        Variant variant = mapper.readValue(new File("~/data150/neo4j/test2.json"), Variant.class);

}
