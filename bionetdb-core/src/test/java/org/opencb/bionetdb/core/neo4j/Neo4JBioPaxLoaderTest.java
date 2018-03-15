package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.biopax.paxtools.io.BioPAXIOHandler;
import org.biopax.paxtools.io.SimpleIOHandler;
import org.biopax.paxtools.model.BioPAXElement;
import org.biopax.paxtools.model.Model;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class Neo4JBioPaxLoaderTest {

    @Test
    public void browseBioPaxFile() throws Exception {
        String filename = getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").getPath();
        Path path = Paths.get(filename);

        // Reading GZip input stream
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
        } else {
            inputStream = Files.newInputStream(path);
        }

        // Retrieving model from BioPAX file
        BioPAXIOHandler handler = new SimpleIOHandler();
        Model model = handler.convertFromOWL(inputStream);

        // Retrieving BioPAX element
        Set<BioPAXElement> bioPAXElements = model.getObjects();
        Iterator<BioPAXElement> iterator = bioPAXElements.iterator();

        Map<String, Long> labelMap = new HashMap<>();
        while (iterator.hasNext()) {
            BioPAXElement bioPAXElement = iterator.next();
            String label = bioPAXElement.getModelInterface().getSimpleName();
            if (StringUtils.isNotEmpty(label)) {
                if (!labelMap.containsKey(label)) {
                    labelMap.put(label, 1L);
                } else {
                    labelMap.put(label, labelMap.get(label) + 1);
                }
            }
        }
        long total = 0;
        for (String label: labelMap.keySet()) {
            System.out.println(label + "\t" + labelMap.get(label));
            total += labelMap.get(label);
        }
        System.out.println("Total: keys = " + labelMap.size() + ", values = " + total);
    }

}