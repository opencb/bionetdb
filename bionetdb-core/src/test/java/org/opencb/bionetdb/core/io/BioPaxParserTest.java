package org.opencb.bionetdb.core.io;

import org.junit.Test;
import org.opencb.bionetdb.core.models.Network;

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

        assertEquals("Different number of physical entities: ", 4758, network.getPhysicalEntities().size());
        assertEquals("Different number of interactions: ", 1971, network.getInteractions().size());
    }
}