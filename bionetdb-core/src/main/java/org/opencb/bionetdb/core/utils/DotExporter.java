package org.opencb.bionetdb.core.utils;

import org.opencb.biodata.formats.graph.dot.Dot;
import org.opencb.biodata.formats.graph.dot.Edge;
import org.opencb.biodata.formats.graph.dot.Node;
import org.opencb.bionetdb.core.models.Interaction;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.PhysicalEntity;
import org.opencb.bionetdb.core.models.*;

import java.util.HashMap;

/**
 * Created by imedina on 30/09/15.
 */
public class DotExporter {

    public static Dot convert(Network network) {

        Dot dot = new Dot("network", true);

        // Creating PhysicalEntities
        for (PhysicalEntity physicalEntity : network.getPhysicalEntities()) {
            dot.addNode(new Node(physicalEntity.getId(), new HashMap<>()));
        }

        // Creating Interactions
        for (Interaction interaction : network.getInteractions()) {
            dot.addNode(new Node(interaction.getId(), new HashMap<>()));
        }

        // Connecting PhysicalEntities and Interactions
        for (Interaction interaction : network.getInteractions()) {
            switch(interaction.getType()) {
                case REACTION:
                    Reaction reaction = (Reaction) interaction;
                    for (String product : reaction.getProducts()) {
                        dot.addEdge(new Edge(reaction.getId(), product));
                    }
                    for (String reactant : reaction.getReactants()) {
                        dot.addEdge(new Edge(reactant, reaction.getId()));
                    }
                    break;
                case CATALYSIS:
                    Catalysis catalysis = (Catalysis) interaction;
                    for (String controller : catalysis.getControllers()) {
                        dot.addEdge(new Edge(controller, catalysis.getId()));
                    }
                    for (String process : catalysis.getControlledProcesses()) {
                        dot.addEdge(new Edge(catalysis.getId(), process));
                    }
                    break;
                case REGULATION:
                    Regulation regulation = (Regulation) interaction;
                    for (String controller : regulation.getControllers()) {
                        dot.addEdge(new Edge(controller, regulation.getId()));
                    }
                    for (String process : regulation.getControlledProcesses()) {
                        dot.addEdge(new Edge(regulation.getId(), process));
                    }
                    break;
                case COLOCALIZATION:
                    // TODO
                    break;
                default:
                    break;
            }
        }

        // Creating and connecting Xrefs
        for (PhysicalEntity physicalEntity : network.getPhysicalEntities()) {
            for (Xref xref : physicalEntity.getXrefs()) {
                String xrefName = xref.getSource() + ":" + xref.getId();
                dot.addNode(new Node(xrefName, new HashMap<>()));
                dot.addEdge(new Edge(xrefName, physicalEntity.getId()));
            }
        }

        // Creating and connecting CellularLocations
        for (PhysicalEntity physicalEntity : network.getPhysicalEntities()) {
            for (CellularLocation cellularLocation : physicalEntity.getCellularLocation()) {
                dot.addNode(new Node(cellularLocation.getName(), new HashMap<>()));
                dot.addEdge(new Edge(cellularLocation.getName(), physicalEntity.getId()));

                for (Ontology ontology : cellularLocation.getOntologies()) {
                    String ontologyName = ontology.getSource() + ":" + ontology.getId();
                    dot.addNode(new Node(ontologyName, new HashMap<>()));
                    dot.addEdge(new Edge(ontologyName, cellularLocation.getName()));
                }
            }
        }
        return dot;
    }
}
