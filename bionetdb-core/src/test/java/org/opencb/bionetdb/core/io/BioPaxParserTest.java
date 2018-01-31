package org.opencb.bionetdb.core.io;

import org.junit.Test;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 17/08/15.
 */
public class BioPaxParserTest {

    @Test
    public void testParse() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);

        int total;

        System.out.println("Nodes:");
        System.out.println("------");
        Map<Node.Type, Integer> typeCounter = new HashMap<>();
        for (Node node: network.getNodes()) {
            if (!typeCounter.containsKey(node.getType())) {
                typeCounter.put(node.getType(), 0);
            }
            typeCounter.put(node.getType(), typeCounter.get(node.getType()) + 1);
        }
        total = 0;
        for (Node.Type key: typeCounter.keySet()) {
            System.out.println(key.name() + ": " + typeCounter.get(key));
            total += typeCounter.get(key);
        }
        System.out.println("\nNumber of nodes: " + total + "\n");

        total = 0;
        System.out.println("Relationships:");
        System.out.println("------");
        Map<Relation.Type, Integer> relTypeCounter = new HashMap<>();
        for (Relation relation: network.getRelations()) {
            if (!relTypeCounter.containsKey(relation.getType())) {
                relTypeCounter.put(relation.getType(), 0);
            }
            relTypeCounter.put(relation.getType(), relTypeCounter.get(relation.getType()) + 1);
        }
        total = 0;
        for (Relation.Type key: relTypeCounter.keySet()) {
            System.out.println(key.name() + ": " + relTypeCounter.get(key));
            total += relTypeCounter.get(key);
        }
        System.out.println("\nNumber of relationships: " + total + "\n");
    }
}