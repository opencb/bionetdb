package org.opencb.bionetdb.core.utils;

import org.junit.Test;
import org.opencb.bionetdb.core.models.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by imedina on 30/09/15.
 */
public class DotExporterTest {

    @Test
    public void testConvert() throws Exception {

        Network network = new Network();

        // Creating PEs
        List<Node> physicalEntitiesList = new ArrayList<>();
        physicalEntitiesList.add(new Protein("A", "A", new ArrayList<>()));
        physicalEntitiesList.add(new Protein("B", "B", new ArrayList<>()));
        physicalEntitiesList.add(new Protein("C", "C", new ArrayList<>()));
        physicalEntitiesList.add(new Protein("D", "D", new ArrayList<>()));
        physicalEntitiesList.add(new Protein("E", "E", new ArrayList<>()));
        physicalEntitiesList.add(new Protein("F", "F", new ArrayList<>()));
        network.setNodes(physicalEntitiesList);

        // Creating Interactions
        List<Relation> interactionsList = new ArrayList<>();
        Reaction reaction1 = new Reaction("ABCD", "ABCD", new ArrayList<>(), Reaction.ReactionType.REACTION);
        Catalysis catalysis1 = new Catalysis("EABCD", "EABCD", new ArrayList<>());
        Regulation regulation1 = new Regulation("DEABCD", "DEABCD", new ArrayList<>());
        Reaction reaction2 = new Reaction("DF", "DF", new ArrayList<>(), Reaction.ReactionType.REACTION);

        reaction1.getReactants().add("A");
        reaction1.getReactants().add("B");
        reaction1.getProducts().add("C");
        reaction1.getProducts().add("D");

        catalysis1.getControllers().add("E");
        catalysis1.getControlledProcesses().add("ABCD");

        regulation1.getControllers().add("D");
        regulation1.getControlledProcesses().add("EABCD");

        reaction2.getReactants().add("D");
        reaction2.getProducts().add("F");

        interactionsList.add(reaction1);
        interactionsList.add(catalysis1);
        interactionsList.add(regulation1);
        interactionsList.add(reaction2);

        network.setRelations(interactionsList);

        assertEquals("Dot file: ",
                "digraph \"network\" {\n" +
                        "\tA;\n" +
                        "\tB;\n" +
                        "\tDF;\n" +
                        "\tC;\n" +
                        "\tD;\n" +
                        "\tE;\n" +
                        "\tF;\n" +
                        "\tDEABCD;\n" +
                        "\tABCD;\n" +
                        "\tEABCD;\n" +
                        "\tABCD->C;\n" +
                        "\tABCD->D;\n" +
                        "\tA->ABCD;\n" +
                        "\tB->ABCD;\n" +
                        "\tE->EABCD;\n" +
                        "\tEABCD->ABCD;\n" +
                        "\tD->DEABCD;\n" +
                        "\tDEABCD->EABCD;\n" +
                        "\tDF->F;\n" +
                        "\tD->DF;\n" +
                        "}\n",
                DotExporter.convert(network).toString());
    }
}
