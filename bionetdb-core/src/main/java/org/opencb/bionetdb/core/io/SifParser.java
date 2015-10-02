package org.opencb.bionetdb.core.io;

import org.opencb.bionetdb.core.models.*;

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
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
        } else {
            inputStream = Files.newInputStream(path);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line = br.readLine();
        while (line != null) {
            List<String> splitLine = Arrays.asList(line.split("\t"));

            // Create PhysicalEntity and Interaction with basic info
            PhysicalEntity pe1 = createPhysicalEntity("pe" + splitLine.get(0));
            PhysicalEntity pe2 = createPhysicalEntity("pe" + splitLine.get(2));
            PhysicalEntity product = createPhysicalEntity(pe1.getId() + "_" + pe2.getId());
            Reaction assembly = createAssembly("re_" + pe1.getId() + "_" + pe2.getId());

            // Set participants
            pe1.getParticipantOfInteraction().add(assembly.getId());
            pe2.getParticipantOfInteraction().add(assembly.getId());
            product.getParticipantOfInteraction().add(assembly.getId());
            assembly.getParticipants().add(pe1.getId());
            assembly.getParticipants().add(pe2.getId());
            assembly.getParticipants().add(product.getId());
            assembly.getReactants().add(pe1.getId());
            assembly.getReactants().add(pe2.getId());
            assembly.getProducts().add(product.getId());

            // Add to network
            network.setPhysicalEntity(pe1);
            network.setPhysicalEntity(pe2);
            network.setPhysicalEntity(product);
            network.setInteraction(assembly);

            line = br.readLine();
        }
        return network;
    }

    private PhysicalEntity createPhysicalEntity(String physicalEntitySIF) {
        Undefined undefined = new Undefined();

        // Id
        undefined.setId(physicalEntitySIF);

        // Name
        undefined.setName(physicalEntitySIF);

        // Xref
        Xref xref = new Xref();
        // TODO change source to user source input
        xref.setSource("unknown");
        xref.setId(physicalEntitySIF);
        undefined.setXref(xref);

        return undefined;
    }

    private Reaction createAssembly(String relationship) {
        Reaction assembly = new Reaction(Reaction.ReactionType.ASSEMBLY);

        // Id
        assembly.setId(relationship);

        // Name
        assembly.setName(relationship);

        // Xref
        Xref xref = new Xref();
        // TODO change source to user source input
        xref.setSource("unknown");
        xref.setId(relationship);
        assembly.setXref(xref);

        return assembly;
    }

}
