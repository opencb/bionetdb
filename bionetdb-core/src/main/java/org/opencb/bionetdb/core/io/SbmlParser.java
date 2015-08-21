package org.opencb.bionetdb.core.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.bionetdb.core.models.Network;
import org.sbml.libsbml.*;
import java.io.IOException;
import java.nio.file.Path;


/**
 * Created by imedina on 12/08/15.
 */
public class SbmlParser {

    /**
     * The following static block is needed in order to load the
     * libSBML Java interface library when the application starts.
     */
    static {
        try {
            System.loadLibrary("sbmlj");
            // For extra safety, check that the jar file is in the classpath.
            Class.forName("org.sbml.libsbml.libsbml");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Error encountered while attempting to load libSBML:");
            System.err.println("Please check the value of your "
                    + (System.getProperty("os.name").startsWith("Mac OS")
                    ? "DYLD_LIBRARY_PATH" : "LD_LIBRARY_PATH") +
                    " environment variable and/or your" +
                    " 'java.library.path' system property (depending on" +
                    " which one you are using) to make sure it list the" +
                    " directories needed to find the " +
                    System.mapLibraryName("sbmlj") + " library file and" +
                    " libraries it depends upon (e.g., the XML parser).");
            System.exit(1);
        } catch (ClassNotFoundException e) {
            System.err.println("Error: unable to load the file 'libsbmlj.jar'." +
                    " It is likely that your -classpath command line " +
                    " setting or your CLASSPATH environment variable " +
                    " do not include the file 'libsbmlj.jar'.");
            e.printStackTrace();
            System.exit(1);
        } catch (SecurityException e) {
            System.err.println("Error encountered while attempting to load libSBML:");
            e.printStackTrace();
            System.err.println("Could not load the libSBML library files due to a"+
                    " security exception.\n");
            System.exit(1);
        }
    }


    private ObjectMapper objectMapper;

    private static final String REACTOME_FEAT = "reactome.";

    public SbmlParser() {
        init();
    }

    private void init() {
    }

    public Network parse(Path path) throws IOException {
        Network network = new Network();

        // Retrieving model from BioPAX file
        SBMLReader reader = new SBMLReader();
        SBMLDocument sbml = reader.readSBML(path.toFile().getAbsolutePath());
        Model model = sbml.getModel();

        // Species
        ListOfSpecies listOfSpecies= model.getListOfSpecies();
        for (int i=0; i < model.getNumSpecies(); i++) {
            Species species = model.getSpecies(i);
            network.getPhysicalEntities().add(createPhysicalEntity(species));
        }

        // Reactions
        ListOfReactions listOfReactions= model.getListOfReactions();
        for (int i=0; i < model.getNumReactions(); i++) {
            Reaction reaction = model.getReaction(i);
            network.getInteractions().add(createInteraction(reaction));
        }

        return network;
    }

    private org.opencb.bionetdb.core.models.PhysicalEntity createPhysicalEntity(Species species) {
        org.opencb.bionetdb.core.models.PhysicalEntity physicalEntity =
                new org.opencb.bionetdb.core.models.PhysicalEntity();


        physicalEntity.setId(species.getId());

        species.getName();
        species.getCompartment();

        species.getAnnotation();

        species.getNotes();



        return physicalEntity;
    }

    private org.opencb.bionetdb.core.models.Interaction createInteraction(Reaction reaction) {
        org.opencb.bionetdb.core.models.Interaction interaction =
                new org.opencb.bionetdb.core.models.Interaction();

        // TODO

        return interaction;
    }

}
