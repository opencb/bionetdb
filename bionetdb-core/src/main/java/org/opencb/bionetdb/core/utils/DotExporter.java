package org.opencb.bionetdb.core.utils;

import org.opencb.biodata.formats.graph.dot.Dot;
import org.opencb.biodata.formats.graph.dot.Edge;
import org.opencb.biodata.formats.graph.dot.Node;
import org.opencb.bionetdb.core.models.*;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.core.network.Relation;

import java.util.HashMap;

/**
 * Created by imedina on 30/09/15.
 */
public class DotExporter {

    public static Dot convert(Network network) {

        Dot dot = new Dot("network", true);

        // Creating Nodes
        for (org.opencb.bionetdb.core.network.Node node: network.getNodes()) {
            dot.addNode(new Node(node.getId(), new HashMap<>()));
        }

        // Creating Relationships
        for (Relation relation : network.getRelations()) {
            dot.addNode(new Node(relation.getId(), new HashMap<>()));
        }

        // Connecting Nodes and Relationships
        for (Relation relation : network.getRelations()) {
            switch(relation.getType()) {
                case REACTION:
                    Reaction reaction = (Reaction) relation;
                    for (String product : reaction.getProducts()) {
                        dot.addEdge(new Edge(reaction.getId(), product));
                    }
                    for (String reactant : reaction.getReactants()) {
                        dot.addEdge(new Edge(reactant, reaction.getId()));
                    }
                    break;
                case CATALYSIS:
                    Catalysis catalysis = (Catalysis) relation;
                    for (String controller : catalysis.getControllers()) {
                        dot.addEdge(new Edge(controller, catalysis.getId()));
                    }
                    for (String process : catalysis.getControlledProcesses()) {
                        dot.addEdge(new Edge(catalysis.getId(), process));
                    }
                    break;
                case REGULATION:
                    Regulation regulation = (Regulation) relation;
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
        for (org.opencb.bionetdb.core.network.Node node: network.getNodes()) {
            if (org.opencb.bionetdb.core.network.Node.isPhysicalEntity(node)) {
                PhysicalEntity physicalEntity = (PhysicalEntity) node;
                for (Xref xref: physicalEntity.getXrefs()) {
                    String xrefName = xref.getSource() + "_" + xref.getId();
                    dot.addNode(new Node(xrefName, new HashMap<>()));
                    dot.addEdge(new Edge(xrefName, physicalEntity.getId()));
                }
            }
        }

        // Creating and connecting CellularLocations
        for (org.opencb.bionetdb.core.network.Node node: network.getNodes()) {
            if (org.opencb.bionetdb.core.network.Node.isPhysicalEntity(node)) {
                PhysicalEntity physicalEntity = (PhysicalEntity) node;
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
        }
        return dot;
    }
}
