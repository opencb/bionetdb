package org.opencb.bionetdb.core.io;

/**
 * Created by dapregi on 8/09/15.
 */
public class PsiMiTabParser {
    //TODO: update this code according to the refactoring Network
/*
    private static final String INTACT_FEAT = "intact.";

    // http://stackoverflow.com/questions/406230/regular-expression-to-match-line-that-doesnt-contain-a-word
    private static final Pattern REACTION_PATTERN =
            Pattern.compile("(?m)^(?=.*reaction|.*cleavage|lipid addition"
                    + "|.*elongation|phosphopantetheinylation)((?!transglutamination reaction).)*$");

    private static final Pattern ASSEMBLY_PATTERN =
            Pattern.compile("(.*association|direct interaction|"
                    + "transglutamination reaction|disulfide bond|covalent binding)");

    public PsiMiTabParser() {
        init();
    }

    private void init() {
    }

    public Network parse(Path path, String species) throws IOException {
        throw new IOException("Not yet implemented");
    }

    public Network parseOLD(Path path, String species) throws IOException {
        // TODO: update code to the new Network and NetworkManager refactoring
        Network network = new Network();

        // Reading GZip input stream
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
        } else {
            inputStream = Files.newInputStream(path);
        }

        // Retrieving species of interest
        Set<String> speciesSet = new HashSet<>(Arrays.asList(species.split(",")));

        // Iterate through PSI-MI TAB interactions
        PsimiTabReader psiMiTabReader = new PsimiTabReader();
        Iterator<BinaryInteraction> iterator = psiMiTabReader.iterate(inputStream);
        boolean isSpeciesA = false;
        boolean isSpeciesB = false;
        while (iterator.hasNext()) {
            BinaryInteraction binaryInteraction = iterator.next();

            isSpeciesA = false;
            isSpeciesB = false;

            if (binaryInteraction.getInteractorA() == null || binaryInteraction.getInteractorB() == null) {
                continue;
            }

            // Check if interactors are from the desired species
            Collection<CrossReference> crA = binaryInteraction.getInteractorA().getOrganism().getIdentifiers();
            for (CrossReference crAElement : crA) {
                if (speciesSet.contains(crAElement.getText())) {
                    isSpeciesA = true;
                    break;
                }
            }
            Collection<CrossReference> crB = binaryInteraction.getInteractorB().getOrganism().getIdentifiers();
            for (CrossReference crBElement : crB) {
                if (speciesSet.contains(crBElement.getText())) {
                    isSpeciesB = true;
                    break;
                }
            }

            if (isSpeciesA && isSpeciesB) {

                PhysicalEntity peA = createPhysicalEntity(binaryInteraction.getInteractorA());
                PhysicalEntity peB = createPhysicalEntity(binaryInteraction.getInteractorB());

                Interaction interaction = createInteraction(binaryInteraction);

                // source
                List<CrossReference> cRs = binaryInteraction.getSourceDatabases();
                for (CrossReference cR : cRs) {
                    if (cR.getDatabase() != null) {
                        peA.getSource().add(cR.getDatabase());
                        peB.getSource().add(cR.getDatabase());
                        interaction.getSource().add(cR.getDatabase());
                    }
                }

                network.addNode(peA);
                network.addNode(peB);
                network.addRelation(interaction);
            }
        }

        // Creating undefined products for each interaction
        createUndefinedProducts(network);

        // Setting for each PE the interactions where they participate
        setParticipantOfInteraction(network);

        return network;
    }

    private PhysicalEntity createPhysicalEntity(Interactor interactor) {
        PhysicalEntity physicalEntity = null;

        String interactorType = "undefined";
        List<CrossReference> crossReferenceListInteractorTypes = interactor.getInteractorTypes();
        for (CrossReference crossReference : crossReferenceListInteractorTypes) {
            interactorType = crossReference.getText();
        }

        switch (interactorType) {
            case "gene":
            case "deoxyribonucleic acid":
            case "single stranded deoxyribonucleic acid":
            case "double stranded deoxyribonucleic acid":
                physicalEntity = createDna(interactor);
                break;
            case "nucleic acid":
                // NOPE
                break;
            case "ribonucleic acid":
            case "messenger rna":
            case "transfer rna":
            case "small nuclear rna":
            case "ribosomal rna":
            case "long non-coding ribonucleic acid":
                physicalEntity = createRna(interactor);
                break;
            case "protein":
            case "peptide":
                physicalEntity = createProtein(interactor);
                break;
            case "complex":
                physicalEntity = createComplex(interactor);
                break;
            case "small molecule":
            case "poly adenine":
                physicalEntity = createSmallMolecule(interactor);
                break;
            case "undefined":
                physicalEntity = createUndefinedEntity(interactor);
                break;
            default:
                break;

        }
        return physicalEntity;
    }

    private Dna createDna(Interactor interactor) {
        Dna dna = new Dna();

        // Common properties
        setPhysicalEntityCommonProperties(interactor, dna);

        return dna;
    }

    private Rna createRna(Interactor interactor) {
        Rna rna = new Rna();

        String interactorType = "undefined";
        List<CrossReference> crossReferenceListInteractorTypes = interactor.getInteractorTypes();
        for (CrossReference crossReference : crossReferenceListInteractorTypes) {
            interactorType = crossReference.getText();
        }

        switch (interactorType) {
            case "ribonucleic acid":
                break;
            case "messenger rna":
                rna.setRnaType(Rna.RnaType.MRNA);
                break;
            case "transfer rna":
                rna.setRnaType(Rna.RnaType.TRNA);
                break;
            case "small nuclear rna":
                rna.setRnaType(Rna.RnaType.SNRNA);
                break;
            case "ribosomal rna":
                rna.setRnaType(Rna.RnaType.RRNA);
                break;
            case "long non-coding ribonucleic acid":
                rna.setRnaType(Rna.RnaType.LNCRNA);
                break;
            default:
                break;
        }

        // Common properties
        setPhysicalEntityCommonProperties(interactor, rna);

        return rna;
    }

    private Protein createProtein(Interactor interactor) {
        Protein protein = new Protein();

        // Common properties
        setPhysicalEntityCommonProperties(interactor, protein);

        // isPeptide
        String interactorType = "";
        List<CrossReference> crossReferenceListInteractorTypes = interactor.getInteractorTypes();
        for (CrossReference crossReference : crossReferenceListInteractorTypes) {
            interactorType = crossReference.getText();
        }
        if (interactorType.equals("peptide")) {
            protein.setPeptide(true);
        }

        // Domains
        for (CrossReference cR : interactor.getXrefs()) {
            if (cR.getDatabase() != null) {
                String source = cR.getDatabase();
                if (source.equals("interpro") || source.equals("rcsb pdb")) {
                    Xref xref = new Xref();
                    xref.setSource(cR.getDatabase());
                    xref.setId(cR.getIdentifier());

                    Domain domain = new Domain();
                    if (cR.getText() != null) {
                        domain.setName(cR.getText());
                    }
                    domain.setXref(xref);

                    protein.getDomains().add(domain);
                }
            }
        }

        return protein;
    }

    private Complex createComplex(Interactor interactor) {
        Complex complex = new Complex();

        // Common properties
        setPhysicalEntityCommonProperties(interactor, complex);

        return complex;
    }

    private SmallMolecule createSmallMolecule(Interactor interactor) {
        SmallMolecule smallMolecule = new SmallMolecule();

        // Common properties
        setPhysicalEntityCommonProperties(interactor, smallMolecule);

        return smallMolecule;
    }

    private Undefined createUndefinedEntity(Interactor interactor) {
        Undefined undefined = new Undefined();

        // Common properties
        setPhysicalEntityCommonProperties(interactor, undefined);

        return undefined;
    }

    private void setPhysicalEntityCommonProperties(Interactor interactor, PhysicalEntity physicalEntity) {

        // id
        physicalEntity.setId(interactor.getIdentifiers().get(0).getIdentifier());

        // xrefs
        List<CrossReference> cRList = new ArrayList<>();
        cRList.addAll(interactor.getXrefs());
        cRList.addAll(interactor.getAlternativeIdentifiers());
        for (CrossReference cR : cRList) {
            if (cR.getDatabase() != null) {
                String source = cR.getDatabase().toLowerCase();
                if (source.equals("sbo") || source.equals("go") || source.equals("mi") || source.equals("ec")) {
                    Ontology ontology = new Ontology();
                    ontology.setSource(cR.getDatabase());
                    ontology.setId(cR.getIdentifier());
                    ontology.setName(cR.getText());
                    physicalEntity.setOntology(ontology);
                } else if (source.equals("pubmed")) {
                    Publication publication = new Publication();
                    publication.setSource(cR.getDatabase());
                    publication.setId(cR.getIdentifier());
                    physicalEntity.setPublication(publication);
                } else if (source.equals("interpro") || source.equals("rcsb pdb")) {
                    continue; // This info will be retrieved in createProtein() method
                } else {
                    Xref xref = new Xref();
                    xref.setSource(cR.getDatabase());
                    xref.setId(cR.getIdentifier());
                    physicalEntity.setXref(xref);
                }
            }
        }
        for (Alias alias : interactor.getAliases()) {
            Xref xref = new Xref();
            xref.setSource(alias.getDbSource());
            xref.setId(alias.getName());
            physicalEntity.setXref(xref);
        }
        for (CrossReference id : interactor.getIdentifiers()) {
            Xref xref = new Xref();
            xref.setSource(id.getDatabase());
            xref.setId(id.getIdentifier());
            physicalEntity.setXref(xref);
        }

        // name
        physicalEntity.setName(physicalEntity.getId()); // default
        for (Xref xref : physicalEntity.getXrefs()) {
            if (xref.getSource().equals("psi-mi")) {
                physicalEntity.setName(xref.getId());
            }
        }


        // comments
        List<String> comments = new ArrayList<>();
        for (Annotation annotation : interactor.getAnnotations()) {
            String comment = annotation.getText();
            if (comment != null && (comment.equals("mint") || comment.equals("homomint") || comment.equals("domino"))) {
                continue;
            }
            comments.add(comment);
        }
        physicalEntity.getAttributes().put(INTACT_FEAT + "comment", comments);

        // TODO features

    }

    private Interaction createInteraction(BinaryInteraction binaryInteraction) {
        Interaction interaction = new Interaction();

        String interactionType = "undefined";
        List<CrossReference> crossReferenceListInteractionTypes = binaryInteraction.getInteractionTypes();
        for (CrossReference crossReference : crossReferenceListInteractionTypes) {
            interactionType = crossReference.getText();
        }

        if (ASSEMBLY_PATTERN.matcher(interactionType).matches()) {
            interaction = createAssembly(binaryInteraction);
        } else if (Pattern.matches("colocalization", interactionType)) {
            interaction = createColocalization(binaryInteraction);
        }

        return interaction;
    }

    @Deprecated
    private Reaction createReaction(BinaryInteraction binaryInteraction) {
        Reaction reaction = new Reaction(Reaction.ReactionType.REACTION);

        // Common properties
        setInteractionCommonProperties(binaryInteraction, reaction);

        // name
        reaction.setName("reaction_" + reaction.getId());

        // Stoichiometry
        if (!binaryInteraction.getInteractorA().getStoichiometry().isEmpty()
                && !binaryInteraction.getInteractorB().getStoichiometry().isEmpty()) {
            Map stoichiometryA = new HashMap<String, Object>();
            stoichiometryA.put("component", binaryInteraction.getInteractorA().getIdentifiers().get(0).getIdentifier());
            stoichiometryA.put("coefficient", binaryInteraction.getInteractorA().getStoichiometry());
            reaction.getStoichiometry().add(stoichiometryA);
            Map stoichiometryB = new HashMap<String, Object>();
            stoichiometryB.put("component", binaryInteraction.getInteractorB().getIdentifiers().get(0).getIdentifier());
            stoichiometryB.put("coefficient", binaryInteraction.getInteractorB().getStoichiometry());
            reaction.getStoichiometry().add(stoichiometryB);
        }

        return reaction;
    }

    private Reaction createAssembly(BinaryInteraction binaryInteraction) {
        Reaction assembly = new Reaction(Reaction.ReactionType.ASSEMBLY);

        // Common properties
        setInteractionCommonProperties(binaryInteraction, assembly);

        // name
        assembly.setName("assembly_" + assembly.getId());

        // reactants
        String idA = binaryInteraction.getInteractorA().getIdentifiers().get(0).getIdentifier();
        String idB = binaryInteraction.getInteractorB().getIdentifiers().get(0).getIdentifier();
        assembly.getReactants().add(idA);
        assembly.getReactants().add(idB);

        // product
        String productId = idA + "_" + idB;
        assembly.getProducts().add(productId);
        // adding product to participants
        assembly.getParticipants().add(productId);

        // Stoichiometry
        if (!binaryInteraction.getInteractorA().getStoichiometry().isEmpty()
                && !binaryInteraction.getInteractorB().getStoichiometry().isEmpty()) {
            Map stoichiometryA = new HashMap<String, Object>();
            stoichiometryA.put("component", binaryInteraction.getInteractorA().getIdentifiers().get(0).getIdentifier());
            stoichiometryA.put("coefficient", binaryInteraction.getInteractorA().getStoichiometry());
            assembly.getStoichiometry().add(stoichiometryA);
            Map stoichiometryB = new HashMap<String, Object>();
            stoichiometryB.put("component", binaryInteraction.getInteractorB().getIdentifiers().get(0).getIdentifier());
            stoichiometryB.put("coefficient", binaryInteraction.getInteractorB().getStoichiometry());
            assembly.getStoichiometry().add(stoichiometryB);
        }

        return assembly;
    }

    private Interaction createColocalization(BinaryInteraction binaryInteraction) {
        Interaction colocalization = new Interaction();
        colocalization.setType(Interaction.Type.COLOCALIZATION);

        // Common properties
        setInteractionCommonProperties(binaryInteraction, colocalization);

        // name
        colocalization.setName("colocalization_" + colocalization.getId());

        return colocalization;
    }

    private void setInteractionCommonProperties(BinaryInteraction binaryInteraction, Interaction interaction) {

        // id
        String idA = binaryInteraction.getInteractorA().getIdentifiers().get(0).getIdentifier();
        String idB = binaryInteraction.getInteractorB().getIdentifiers().get(0).getIdentifier();
        interaction.setId("interaction_" + idA + "_" + idB);

        // participants
        interaction.getParticipants().add(idA);
        interaction.getParticipants().add(idB);

        // xrefs
        List<CrossReference> cRs = binaryInteraction.getXrefs();
        for (CrossReference cR : cRs) {
            if (cR.getDatabase() != null) {
                String source = cR.getDatabase().toLowerCase();
                if (source.equals("go") || source.equals("psi-mi") || source.equals("omim")) {
                    Ontology ontology = new Ontology();
                    ontology.setSource(cR.getDatabase());
                    ontology.setId(cR.getIdentifier());
                    ontology.setName(cR.getText());
                    interaction.setOntology(ontology);
                } else if (source.equals("pubmed")) {
                    Publication publication = new Publication();
                    publication.setSource(cR.getDatabase());
                    publication.setId(cR.getIdentifier());
                    interaction.setPublication(publication);
                } else {
                    Xref xref = new Xref();
                    xref.setSource(cR.getDatabase());
                    xref.setId(cR.getIdentifier());
                    interaction.setXref(xref);
                }
            }
        }

        // publications
        List<CrossReference> pubs = binaryInteraction.getPublications();
        for (CrossReference pub : pubs) {
            Publication publication = new Publication();
            publication.setSource(pub.getDatabase());
            publication.setId(pub.getIdentifier());
            interaction.setPublication(publication);
        }

        // comments
        List<String> comments = new ArrayList<>();
        List<Annotation> annotations = binaryInteraction.getAnnotations();
        for (Annotation annotation : annotations) {
            String comment = annotation.getText();
            if (comment != null && (comment.equals("mint") || comment.equals("homomint") || comment.equals("domino"))) {
                continue;
            }
            comments.add(comment);
        }
        interaction.getAttributes().put(INTACT_FEAT + "comment", comments);
    }

    private void createUndefinedProducts(Network network) {
        // TODO: update code to the new Network and NetworkManager refactoring
//        for (Relation relationship: network.getRelations()) {
//            if (relationship.getType() == Relation.Type.REACTION) {
//                Reaction reaction = (Reaction) relationship;
//                for (String productId : reaction.getProducts()) {
//                    Undefined undefined = new Undefined(productId, productId, Collections.<String>emptyList());
//                    Xref xref = new Xref("BioNetDB", "", productId, "");
//                    undefined.setXref(xref);
//                    network.setNode(undefined);
//                }
//            }
//        }
    }

    private void setParticipantOfInteraction(Network network) {
        // TODO: update code to the new Network and NetworkManager refactoring
//        for (Relation relationship: network.getRelations()) {
//            for (String peId: ((Interaction) relationship).getParticipants()) {
//                ((PhysicalEntity) network.getNode(peId)).getParticipantOfInteraction().add(relationship.getId());
//            }
//        }
    }
    */
}
