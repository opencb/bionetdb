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
        for (int i=0; i < model.getNumSpecies(); i++) {
            Species species = model.getSpecies(i);
            network.setPhysicalEntity(createPhysicalEntity(species, model));
        }

        // Fixing complexes info
        fixComplexesInfo(network);

        // Reactions
        for (int i=0; i < model.getNumReactions(); i++) {
            Reaction reaction = model.getReaction(i);
            network.setInteraction(createInteraction(reaction, model));
        }

        // Adding to PhysicalEntities the interactions where they participate
        fixParticipantOfInteractionInfo(network);

        return network;
    }

    private PhysicalEntity createPhysicalEntity(Species species, Model model) {

        PhysicalEntity physicalEntity = null;

        switch (getClassToConvert(species)) {
            case UNDEFINEDENTITY:
                break;
            case DNA:
                physicalEntity = createDNA(species, model);
                break;
            case RNA:
                physicalEntity = createRNA(species, model);
                break;
            case PROTEIN:
                physicalEntity = createProtein(species, model);
                break;
            case COMPLEX:
                physicalEntity = createComplex(species, model);
                break;
            case SMALLMOLECULE:
                physicalEntity = createSmallMolecule(species, model);
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

            if (res.contains("bind")){
                type = PhysicalEntity.Type.COMPLEX;
            } else if (res.contains("uniprot") || res.contains("interpro") || res.contains("pirsf")) {
                type = PhysicalEntity.Type.PROTEIN;
            } else if (res.contains("kegg") || res.contains("chebi")) {
                type = PhysicalEntity.Type.SMALLMOLECULE;
            } else if (res.contains("ensg")) {
                type = PhysicalEntity.Type.DNA;
            } else if (res.contains("enst")) {
                type = PhysicalEntity.Type.RNA;
            }
        }

        if (description.hasChild("hasPart")) {
            type = PhysicalEntity.Type.COMPLEX;
        }

        return type;
    }

    private Dna createDNA(Species species, Model model) {
        Dna dna = new Dna();

        // Common properties
        setPhysicalEntityCommonProperties(dna, species, model);

        return dna;
    }

    private Rna createRNA(Species species, Model model) {
        Rna rna = new Rna();

        // Common properties
        setPhysicalEntityCommonProperties(rna, species, model);

        return rna;
    }

    private Protein createProtein(Species species, Model model) {
        Protein protein = new Protein();

        // Common properties
        setPhysicalEntityCommonProperties(protein, species, model);

        return protein;
    }

    private Complex createComplex(Species species, Model model) {
        Complex complex = new Complex();

        // Common properties
        setPhysicalEntityCommonProperties(complex, species, model);

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

    private SmallMolecule createSmallMolecule(Species species, Model model) {
        SmallMolecule smallMolecule = new SmallMolecule();

        // Common properties
        setPhysicalEntityCommonProperties(smallMolecule, species, model);

        return smallMolecule;
    }

    private void setPhysicalEntityCommonProperties(PhysicalEntity physicalEntity, Species species, Model model) {
        // id
        physicalEntity.setId(species.getId());

        // name
        physicalEntity.setName(species.getName());

        // cellular location
        physicalEntity.setCellularLocation(getCompartmentInfo(model.getCompartment(species.getCompartment())));

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
                    xref.setDb("kegg");
                } else {
                    xref.setDb(xrefElements.get(0).toLowerCase());
                }
                xref.setId(xrefElements.get(1));
                physicalEntity.getXrefs().add(xref);
            }
        }

        Xref xref = new Xref();
        List<String> sboElements = Arrays.asList(species.getSBOTermID().split(":"));
        xref.setDb(sboElements.get(0).toLowerCase());
        xref.setId(sboElements.get(1));
        physicalEntity.getXrefs().add(xref);

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

    private Map<String, List<String>> getCompartmentInfo (Compartment compartment) {

        Map<String, List<String>> compartmentInfo = new HashMap<>();

        compartmentInfo.put("name", Collections.singletonList(compartment.getName()));

        String id = compartment.getAnnotation().getChild("RDF").getChild("Description").getChild("is")
                .getChild("Bag").getChild("li").getAttributes().getValue("resource");

        // From "urn:miriam:obo.go:GO%3A0005759" to "GO:0005759"
        // Fixing bad formatted colon: from "%3A" to ":"
        List<String> idElements = Arrays.asList(id.replace("%3A", ":").split(":"));
        compartmentInfo.put("id", Collections.singletonList(String.join(":", idElements.subList(idElements.size() - 2, idElements.size()))));

        return compartmentInfo;
    }

    private void fixComplexesInfo(Network network) {
        /**
         * This method transforms the xrefs from the complex attribute "components" into their
         * specific ids.
         *
         * This method also populates the "componentOfComplex" attribute of the physical entities
         * which are part of the complex
         */

        List<PhysicalEntity> physicalEntities = network.getPhysicalEntities();

        // Creating two related lists for every physical entity: xref and its corresponding id
        List<String> xrefs = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (PhysicalEntity physicalEntity : physicalEntities) {
            for (Xref peXref : physicalEntity.getXrefs()) {
                xrefs.add(peXref.getDb() + peXref.getDbVersion() + ":" + peXref.getId() + peXref.getIdVersion());
                ids.add(physicalEntity.getId());
            }
        }

        // Populating "components" and "componentOfComplex" attributes
        for (PhysicalEntity physicalEntity : physicalEntities) {
            if (physicalEntity.getType() == PhysicalEntity.Type.COMPLEX) {
                Complex complex = (Complex) physicalEntity;
                for (String component : complex.getComponents()) {
                    String componentId = component.toLowerCase();
                    if (xrefs.contains(componentId)) {
                        complex.getComponents().set(complex.getComponents().indexOf(component),
                                ids.get(xrefs.indexOf(componentId)));
                        network.getPhysicalEntity(ids.get(xrefs.indexOf(componentId))).getComponentOfComplex().add(complex.getId());
                    }
                }
            }
        }
    }

    private void fixParticipantOfInteractionInfo(Network network) {
        for (Interaction interaction : network.getInteractions()) {
            for (String participant : interaction.getParticipants()) {
                network.getPhysicalEntity(participant).getParticipantOfInteraction().add(interaction.getId());
            }
        }
    }

    private Interaction createInteraction(Reaction reactionSBML, Model model) {
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
                    idXref.setDb("kegg");
                } else {
                    idXref.setDb(xrefElements.get(0).toLowerCase());
                }
                idXref.setId(xrefElements.get(1));
                reaction.getXrefs().add(idXref);
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
                evXref.setDb(xrefElements.get(0).toLowerCase());
                evXref.setId(xrefElements.get(1));
                reaction.getXrefs().add(evXref);
            }
        }

        // reversible
        reaction.setReversible(reactionSBML.getReversible());

        // reactants
        for (int i=0; i < reactionSBML.getNumReactants(); i++) {
            SpeciesReference reactant = reactionSBML.getReactant(i);
            reaction.getReactants().add(reactant.getSpecies());
        }

        // products
        for (int i=0; i < reactionSBML.getNumProducts(); i++) {
            SpeciesReference product = reactionSBML.getProduct(i);
            reaction.getProducts().add(product.getSpecies());
        }

        // controlledBy
        for (int i=0; i < reactionSBML.getNumModifiers(); i++) {
            ModifierSpeciesReference modifier = reactionSBML.getModifier(i);
            reaction.getControlledBy().add(modifier.getSpecies());
        }

        // participants
        reaction.getParticipants().addAll(reaction.getReactants());
        reaction.getParticipants().addAll(reaction.getProducts());
        reaction.getParticipants().addAll(reaction.getControlledBy());

        // processOfPathway
        reaction.getProcessOfPathway().add(model.getId());

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

}
