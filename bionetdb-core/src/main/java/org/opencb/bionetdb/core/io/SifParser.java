package org.opencb.bionetdb.core.io;

import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.network.Relation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by dapregi on 30/09/15.
 */
public class SifParser {

    public SifParser() {
        init();
    }

    private void init() {
    }

    public Network parse(Path path) throws IOException {
        Network network = new Network();

        // Reading GZip input stream
        String source = null;
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
            source = path.toFile().getName();
        } else {
            inputStream = Files.newInputStream(path);
        }

        long uid = 0;

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line = br.readLine();
        while (line != null) {
            List<String> splitLine = Arrays.asList(line.split("\t"));

            // create nodes
            Node pe1 = new Node((uid++), splitLine.get(0), null, Node.Type.PHYSICAL_ENTITY, source);
            Node pe2 = new Node((uid++), splitLine.get(2), null, Node.Type.PHYSICAL_ENTITY, source);

            // create relation
            Relation relation = new Relation((uid++), splitLine.get(1), pe1.getUid(), pe2.getUid(), Relation.Type.INTERACTION, source);

            // Add to network
            network.addNode(pe1);
            network.addNode(pe2);
            network.addRelation(relation);

            line = br.readLine();
        }
        return network;
    }
}
