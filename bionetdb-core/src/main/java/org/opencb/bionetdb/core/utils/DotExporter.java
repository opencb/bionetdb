package org.opencb.bionetdb.core.utils;

import org.opencb.biodata.formats.graph.dot.Dot;
import org.opencb.biodata.formats.graph.dot.Edge;
import org.opencb.biodata.formats.graph.dot.Node;
import org.opencb.bionetdb.core.models.Network;

import java.util.HashMap;

/**
 * Created by imedina on 30/09/15.
 */
public class DotExporter {

    public static Dot convert(Network network) {

        Dot dot = new Dot("network", true);

        dot.addNode(new Node("A", new HashMap<>()));
        dot.addNode(new Node("B", new HashMap<>()));
        dot.addNode(new Node("C", new HashMap<>()));

        dot.addEdge(new Edge("A", "C"));

        return dot;
    }

}
