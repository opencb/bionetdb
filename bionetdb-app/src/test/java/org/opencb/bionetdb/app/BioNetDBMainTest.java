package org.opencb.bionetdb.app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;

public class BioNetDBMainTest {

    @Test
    public void createCsvClinicalAnalysis() {
        String caPath = "/home/jtarraga/data150/clinicalAnalysis";
        String cmdLine = "~/appl/bionetdb/build/bin/bionetdb.sh create-csv -i " + caPath + "/input/ -o csv/ --clinical-analysis";
    }


    public void createNetworks() {
        long uid = 0;

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        Network network;
        Node node1, node2, node3;
        Relation relation1, relation2, relation3;

        network = new Network("net1", "net1", "Network #1");
        network.setNodes(new ArrayList<>());
        network.setRelations(new ArrayList<>());

        node1 = new Node(uid++, "ENSG00000078808", "SDF4", Node.Type.GENE);
        network.getNodes().add(node1);
        node2 = new Node(uid++, null, "COCA", Node.Type.GENE_DRUG_INTERACTION);
        network.getNodes().add(node2);
        relation1 = new Relation(uid++, "rel1", node1.getUid(), Node.Type.GENE, node2.getUid(), Node.Type.GENE_DRUG_INTERACTION,
                Relation.Type.ANNOTATION);
        network.getRelations().add(relation1);

        File file = new File("/tmp/network1.json");
        try {
            network.write(file);
        } catch (FileNotFoundException | JsonProcessingException e) {
            e.printStackTrace();
        }
//        try {
//            mapper.writer().writeValue(, network);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        network = new Network("net2", "net2", "Network #2");
        network.setNodes(new ArrayList<>());
        network.setRelations(new ArrayList<>());

        node1 = new Node(uid++, "ENSG00000066666", "SDF666", Node.Type.GENE);
        network.getNodes().add(node1);
        node2 = new Node(uid++, null, "COCA", Node.Type.GENE_DRUG_INTERACTION);
        network.getNodes().add(node2);
        node3 = new Node(uid++, "ALCOHOL", "ALCOHOL", Node.Type.GENE_DRUG_INTERACTION);
        network.getNodes().add(node3);
        relation2 = new Relation(uid++, "rel2", node1.getUid(), Node.Type.GENE, node2.getUid(), Node.Type.GENE_DRUG_INTERACTION,
                Relation.Type.ANNOTATION);
        network.getRelations().add(relation2);
        relation3 = new Relation(uid++, "rel3", node1.getUid(), Node.Type.GENE, node3.getUid(), Node.Type.GENE_DRUG_INTERACTION,
                Relation.Type.ANNOTATION);
        network.getRelations().add(relation3);

        file = new File("/tmp/network2.json");
        try {
            network.write(file);
        } catch (FileNotFoundException | JsonProcessingException e) {
            e.printStackTrace();
        }
//        try {
//            mapper.writer().writeValue(new File("/tmp/network2.json"), network);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}