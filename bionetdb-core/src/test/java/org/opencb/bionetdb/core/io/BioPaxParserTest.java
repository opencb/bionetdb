package org.opencb.bionetdb.core.io;

import org.junit.Test;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by imedina on 17/08/15.
 */
public class BioPaxParserTest {

    @Test
    public void testParse() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);

        int numPhysicalEntities = 0;
        for (Node node: network.getNodes()) {
            if (Node.isPhysicalEntity(node)) {
                numPhysicalEntities++;
            } else {
                System.out.println(node.getType());
            }
        }
        System.out.println("Number of nodes: " + network.getNodes().size());
        assertEquals("Different number of physical entities: ", 5057, numPhysicalEntities);

        int numInteractions = 0;
        for (Relation relation : network.getRelations()) {
            if (Relation.isInteraction(relation)) {
                numInteractions++;
            }
        }
        System.out.println("Number of relationships: " + network.getRelations().size());
        assertEquals("Different number of interactions: ", 1971, numInteractions);
    }
}