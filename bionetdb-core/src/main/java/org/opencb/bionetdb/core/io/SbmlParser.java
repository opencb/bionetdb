package org.opencb.bionetdb.core.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.bionetdb.core.models.*;
import org.sbml.libsbml.*;
import org.sbml.libsbml.Reaction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
        for (int i = 0; i < model.getNumSpecies(); i++) {
            Species species = model.getSpecies(i);
            network.setPhysicalEntity(createPhysicalEntity(species));
        }

        // Fixing complexes info
        fixComplexesInfo(network);

        // Reactions
        for (int i = 0; i < model.getNumReactions(); i++) {
            Reaction reaction = model.getReaction(i);
            network.setInteraction(createInteraction(reaction, network));
        }

        // Adding to PhysicalEntities the interactions where they participate
        fixParticipantOfInteractionInfo(network);

        return network;
    }

    private PhysicalEntity createPhysicalEntity(Species species) {

        PhysicalEntity physicalEntity = null;

        switch (getClassToConvert(species)) {
            case UNDEFINEDENTITY:
                physicalEntity = createUndefinedEntity(species);
                break;
            case DNA:
                physicalEntity = createDna(species);
                break;
            case RNA:
                physicalEntity = createRna(species);
                break;
            case PROTEIN:
                physicalEntity = createProtein(species);
                break;
            case COMPLEX:
                physicalEntity = createComplex(species);
                break;
            case SMALLMOLECULE:
                physicalEntity = createSmallMolecule(species);
                break;
        }
        return physicalEntity;
    }

    private PhysicalEntity.Type getClassToConvert(Species species) {

        XMLNode description = species.getAnnotation().getChild("RDF").getChild("Description");
        PhysicalEntity.Type type = PhysicalEntity.Type.UNDEFINEDENTITY;

        StringBuilder sb = new StringBuilder();
        if (description.hasChild("is")) {
            XMLNode ids = description.getChild("is").getChild("Bag");
            for (int i = 0; i < ids.getNumChildren(); i++) {
                sb.append(ids.getChild(i).getAttributes().getValue("resource"));
            }
            String res = sb.toString().toLowerCase();

            if (res.contains("uniprot") || res.contains("interpro") || res.contains("pirsf")) {
                type = PhysicalEntity.Type.PROTEIN;
            } else if (res.contains("kegg") || res.contains("chebi")) {
                type = PhysicalEntity.Type.SMALLMOLECULE;
            } else if (res.contains("ensg")) {
                type = PhysicalEntity.Type.DNA;
            } else if (res.contains("enst")) {
                type = PhysicalEntity.Type.RNA;
            } else if (res.contains("bind")) {
                type = PhysicalEntity.Type.COMPLEX;
            }
        }

        if (description.hasChild("hasPart")) {
            type = PhysicalEntity.Type.COMPLEX;
        }

        return type;
    }

    private UndefinedEntity createUndefinedEntity(Species species) {
        UndefinedEntity undefinedEntity = new UndefinedEntity();

        // Common properties
        setPhysicalEntityCommonProperties(undefinedEntity, species);

        return undefinedEntity;
    }

    private Dna createDna(Species species) {
        Dna dna = new Dna();

        // Common properties
        setPhysicalEntityCommonProperties(dna, species);

        return dna;
    }

    private Rna createRna(Species species) {
        Rna rna = new Rna();

        // Common properties
        setPhysicalEntityCommonProperties(rna, species);

        return rna;
    }

    private Protein createProtein(Species species) {
        Protein protein = new Protein();

        // Common properties
        setPhysicalEntityCommonProperties(protein, species);

        return protein;
    }

    private Complex createComplex(Species species) {
        Complex complex = new Complex();

        // Common properties
        setPhysicalEntityCommonProperties(complex, species);

        // Complex properties
        // If description has "hasPart" attribute, the entity is a complex
        XMLNode description = species.getAnnotation().getChild("RDF").getChild("Description");
        if (description.hasChild("hasPart")) {
            XMLNode components = description.getChild("hasPart").getChild("Bag");
            for (int i = 0; i < components.getNumChildren(); i++) {
                String component = components.getChild(i).getAttributes().getValue("resource");
                List<String> componentElements = Arrays.asList(component.replace("%3A", ":").replace("kegg.compound", "kegg").split(":"));
                List<String> componentXrefElements =
                        componentElements.subList(componentElements.size() - 2, componentElements.size());
                complex.getComponents().add(String.join(":", componentXrefElements));
            }
        }
        return complex;
    }

    private SmallMolecule createSmallMolecule(Species species) {
        SmallMolecule smallMolecule = new SmallMolecule();

        // Common properties
        setPhysicalEntityCommonProperties(smallMolecule, species);

        return smallMolecule;
    }

    private void setPhysicalEntityCommonProperties(PhysicalEntity physicalEntity, Species species) {
        // id
        physicalEntity.setId(species.getId());

        // name
        physicalEntity.setName(species.getName());

        // cellular location
        physicalEntity.getCellularLocation().add(getCompartmentInfo(species.getModel().getCompartment(species.getCompartment())));

        // xrefs
        XMLNode description = species.getAnnotation().getChild("RDF").getChild("Description");
        if (description.hasChild("is")) {
            XMLNode ids = description.getChild("is").getChild("Bag");
            for (int i = 0; i < ids.getNumChildren(); i++) {
                Xref xref = new Xref();
                String id = ids.getChild(i).getAttributes().getValue("resource");
                // Fixing bad formatted colon: from "%3A" to ":"
                List<String> idElements = Arrays.asList(id.replace("%3A", ":").split(":"));
                List<String> xrefElements = idElements.subList(idElements.size() - 2, idElements.size());
                if (xrefElements.get(0).contains("kegg.compound")) {
                    xref.setSource("kegg");
                } else {
                    xref.setSource(xrefElements.get(0).toLowerCase());
                }
                xref.setId(xrefElements.get(1));
                physicalEntity.setXref(xref);
            }
        }

        Xref xref = new Xref();
        List<String> sboElements = Arrays.asList(species.getSBOTermID().split(":"));
        xref.setSource(sboElements.get(0).toLowerCase());
        xref.setId(sboElements.get(1));
        physicalEntity.setXref(xref);

        // comments
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < species.getNotes().getNumChildren(); i++) {
            Pattern pattern = Pattern.compile("<.+>(.+)<.+>");
            Matcher matcher = pattern.matcher(species.getNotes().getChild(i).toXMLString());
            if (matcher.matches()) {
                sb.append(matcher.group(1)).append(";;");
            }
        }
        physicalEntity.getAttributes().put(REACTOME_FEAT + "comment", sb.toString());
    }

    private CellularLocation getCompartmentInfo (Compartment compartment) {

        CellularLocation cellularLocation = new CellularLocation();

        // Names
        cellularLocation.getNames().add(compartment.getName());

        // Xrefs
        String cellularLocXref = compartment.getAnnotation().getChild("RDF").getChild("Description")
                .getChild("is").getChild("Bag").getChild("li").getAttributes().getValue("resource");
        // From "urn:miriam:obo.go:GO%3A0005759" to "GO:0005759"
        // Fixing bad formatted colon: from "%3A" to ":"
        List<String> idElements = Arrays.asList(cellularLocXref.replace("%3A", ":").split(":"));

        org.opencb.bionetdb.core.models.Xref xref = new org.opencb.bionetdb.core.models.Xref();
        xref.setSource(idElements.get(idElements.size() - 2));
        xref.setId(idElements.get(idElements.size()-1));
        cellularLocation.setXref(xref);

        return cellularLocation;
    }

    /**
     * This method transforms the xrefs from the complex attribute "components" into their
     * specific IDs. If the ID does not exist, it creates a new PhysicalEntity with that ID.
     *
     * This method also populates the "componentOfComplex" attribute of the physical entities
     * which are part of the complex
     */
    private void fixComplexesInfo(Network network) {
        List<PhysicalEntity> physicalEntities = network.getPhysicalEntities();

        // Creating two related lists for every physical entity: xref and its corresponding id
        List<String> xrefs = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (PhysicalEntity physicalEntity : physicalEntities) {
            for (Xref peXref : physicalEntity.getXrefs()) {
                xrefs.add(peXref.getSource() + peXref.getSourceVersion() + ":" + peXref.getId() + peXref.getIdVersion());
                ids.add(physicalEntity.getId());
            }
        }

        // Populating "components" and "componentOfComplex" attributes
        List<PhysicalEntity> newPhysicalEntities = new ArrayList<>();
        for (PhysicalEntity physicalEntity : physicalEntities) {
            if (physicalEntity.getType() == PhysicalEntity.Type.COMPLEX) {
                Complex complex = (Complex) physicalEntity;
                for (String component : complex.getComponents()) {
                    String componentId = component.replace("%3A", ":").split(":")[0].toLowerCase() + ":" +
                            component.replace("%3A", ":").split(":")[1];
                    if (xrefs.contains(componentId)) {
                        complex.getComponents().set(complex.getComponents().indexOf(component),
                                ids.get(xrefs.indexOf(componentId)));
                        network.getPhysicalEntity(ids.get(xrefs.indexOf(componentId))).getComponentOfComplex().add(complex.getId());
                    } else {
                        // If component xref cannot be transformed into an ID, a new PhysicalEntity is created
                        if (componentId.contains("uniprot") || componentId.contains("interpro") || componentId.contains("pirsf")) {
                            Protein protein = new Protein(componentId, "", "");
                            protein.getComponentOfComplex().add(complex.getId());
                            Xref xref = new Xref(componentId.split(":")[0], "", componentId.split(":")[1], "");
                            protein.setXref(xref);
                            newPhysicalEntities.add(protein);
                        } else if (componentId.contains("kegg") || componentId.contains("chebi")) {
                            SmallMolecule smallMolecule = new SmallMolecule(componentId, "", "");
                            smallMolecule.getComponentOfComplex().add(complex.getId());
                            Xref xref = new Xref(componentId.split(":")[0], "", componentId.split(":")[1], "");
                            smallMolecule.setXref(xref);
                            newPhysicalEntities.add(smallMolecule);
                        } else if (componentId.contains("ensg")) {
                            Dna dna = new Dna(componentId, "", "");
                            dna.getComponentOfComplex().add(complex.getId());
                            Xref xref = new Xref(componentId.split(":")[0], "", componentId.split(":")[1], "");
                            dna.setXref(xref);
                            newPhysicalEntities.add(dna);
                        } else if (componentId.contains("enst")) {
                            Rna rna = new Rna(componentId, "", "");
                            rna.getComponentOfComplex().add(complex.getId());
                            Xref xref = new Xref(componentId.split(":")[0], "", componentId.split(":")[1], "");
                            rna.setXref(xref);
                            newPhysicalEntities.add(rna);
                        } else if (componentId.contains("bind")) {
                            Complex complexx = new Complex(componentId, "", "");
                            complexx.getComponentOfComplex().add(complex.getId());
                            Xref xref = new Xref(componentId.split(":")[0], "", componentId.split(":")[1], "");
                            complexx.setXref(xref);
                            newPhysicalEntities.add(complexx);
                        } else {
                            UndefinedEntity undefinedEntity = new UndefinedEntity(componentId, "", "");
                            undefinedEntity.getComponentOfComplex().add(complex.getId());
                            Xref xref = new Xref(componentId.split(":")[0], "", componentId.split(":")[1], "");
                            undefinedEntity.setXref(xref);
                            newPhysicalEntities.add(undefinedEntity);
                        }
                    }
                }
            }
        }

        if (!newPhysicalEntities.isEmpty()) {
            for (PhysicalEntity newPhysicalEntity : newPhysicalEntities) {
                network.getPhysicalEntities().add(newPhysicalEntity);
            }
        }

    }

    private void fixParticipantOfInteractionInfo(Network network) {
        for (Interaction interaction : network.getInteractions()) {
            for (String participant : interaction.getParticipants()) {
                if (network.getNetworkElementType(participant) == Network.Type.PHYSICALENTITY) {
                    network.getPhysicalEntity(participant).getParticipantOfInteraction().add(interaction.getId());
                }
            }
        }
    }

    private Interaction createInteraction(Reaction reactionSBML, Network network) {
        org.opencb.bionetdb.core.models.Reaction reaction = new org.opencb.bionetdb.core.models.Reaction();

        // id
        reaction.setId(reactionSBML.getId());

        // name
        reaction.setName(reactionSBML.getName());

        // xrefs
        XMLNode description = reactionSBML.getAnnotation().getChild("RDF").getChild("Description");
        if (description.hasChild("is")) {
            XMLNode ids = description.getChild("is").getChild("Bag");
            for (int i = 0; i < ids.getNumChildren(); i++) {
                Xref idXref = new Xref();
                String id = ids.getChild(i).getAttributes().getValue("resource");
                // Fixing bad formatted colon: from "%3A" to ":"
                List<String> idElements = Arrays.asList(id.replace("%3A", ":").split(":"));
                List<String> xrefElements = idElements.subList(idElements.size() - 2, idElements.size());
                if (xrefElements.get(0).contains("kegg.compound")) {
                    idXref.setSource("kegg");
                } else {
                    idXref.setSource(xrefElements.get(0).toLowerCase());
                }
                idXref.setId(xrefElements.get(1));
                reaction.setXref(idXref);
            }
        }

        // evidence
        if (description.hasChild("isDescribedBy")) {
            XMLNode evs = description.getChild("isDescribedBy").getChild("Bag");
            for (int i = 0; i < evs.getNumChildren(); i++) {
                Xref evXref = new Xref();
                String ev = evs.getChild(i).getAttributes().getValue("resource");
                // Fixing bad formatted colon: from "%3A" to ":"
                List<String> evElements = Arrays.asList(ev.replace("%3A", ":").split(":"));
                List<String> xrefElements = evElements.subList(evElements.size() - 2, evElements.size());
                evXref.setSource(xrefElements.get(0).toLowerCase());
                evXref.setId(xrefElements.get(1));
                reaction.setXref(evXref);
            }
        }

        // reversible
        reaction.setReversible(reactionSBML.getReversible());

        // reactants
        for (int i = 0; i < reactionSBML.getNumReactants(); i++) {
            SpeciesReference reactant = reactionSBML.getReactant(i);
            reaction.getReactants().add(reactant.getSpecies());
        }

        // products
        for (int i = 0; i < reactionSBML.getNumProducts(); i++) {
            SpeciesReference product = reactionSBML.getProduct(i);
            reaction.getProducts().add(product.getSpecies());
        }

        // controlledBy
        if (reactionSBML.getNumModifiers() > 0) {
            Catalysis catalysis = createCatalysis(reactionSBML);
            network.setInteraction(catalysis);
            reaction.getControlledBy().add(catalysis.getId());
        }

        // participants
        reaction.getParticipants().addAll(reaction.getReactants());
        reaction.getParticipants().addAll(reaction.getProducts());
        reaction.getParticipants().addAll(reaction.getControlledBy());

        // processOfPathway
        reaction.getProcessOfPathway().add(reactionSBML.getModel().getId());

        // comments
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < reactionSBML.getNotes().getNumChildren(); i++) {
            Pattern pattern = Pattern.compile("<.+>(.+)<.+>");
            Matcher matcher = pattern.matcher(reactionSBML.getNotes().getChild(i).toXMLString());
            if (matcher.matches()) {
                sb.append(matcher.group(1)).append(";;");
            }
        }
        reaction.getAttributes().put(REACTOME_FEAT + "comment", sb.toString());

        return reaction;
    }

    private Catalysis createCatalysis(Reaction reaction) {

        Catalysis catalysis = new Catalysis();

        // id
        catalysis.setId("catalysis_" + reaction.getId());

        // controllers
        for (int i = 0; i < reaction.getNumModifiers(); i++) {
            ModifierSpeciesReference modifier = reaction.getModifier(i);
            catalysis.getControllers().add(modifier.getSpecies());
        }

        // controlledProcesses
        catalysis.getControlledProcesses().add(reaction.getId());

        // processOfPathway
        catalysis.getProcessOfPathway().add(reaction.getModel().getId());

        // participants
        catalysis.getParticipants().addAll(catalysis.getControlledProcesses());
        catalysis.getParticipants().addAll(catalysis.getControllers());

        // controlType
        catalysis.setControlType("ACTIVATION");

        return catalysis;
    }
}
