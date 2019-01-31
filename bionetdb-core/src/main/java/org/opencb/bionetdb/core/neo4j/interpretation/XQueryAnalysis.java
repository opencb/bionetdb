package org.opencb.bionetdb.core.neo4j.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.neo4j.Neo4JVariantIterator;
import org.opencb.bionetdb.core.utils.NodeBuilder;

import java.util.*;
import java.util.concurrent.*;

public class XQueryAnalysis {

    private Driver driver;

    public XQueryAnalysis(Driver driver) {
        this.driver = driver;
    }

    public VariantContainer execute(FamilyFilter familyFilter, GeneFilter geneFilter) throws ExecutionException,
            InterruptedException {
        VariantFilter variantFilter = new VariantFilter();
        OptionsFilter optionsFilter = new OptionsFilter();
        return execute(familyFilter, geneFilter, variantFilter, optionsFilter);
    }

    public VariantContainer execute(FamilyFilter familyFilter, GeneFilter geneFilter, OptionsFilter optionsFilter)
            throws ExecutionException, InterruptedException {
        VariantFilter variantFilter = new VariantFilter();
        return execute(familyFilter, geneFilter, variantFilter, optionsFilter);
    }

    public VariantContainer execute(FamilyFilter familyFilter, GeneFilter geneFilter, VariantFilter variantFilter)
            throws ExecutionException, InterruptedException {
        OptionsFilter optionsFilter = new OptionsFilter();
        return execute(familyFilter, geneFilter, variantFilter, optionsFilter);
    }

    /**
     * This method acts as a manager for XQueryCraftsman. It takes the arguments with the OpenCGA standard and converts them in
     * simpler forms that allow XQueryCraftsman to create the XQuery.
     * <p>
     * IF YOU WANT TO AVOID USING MULTITHREADING JUST TURN "true" THE FIRST "if loop" IN THE "M U L T I T H R E A D E D - X - Q U E R Y"
     * PART
     *
     * @param familyFilter  it is an object that gathers the data of the proband and his family
     * @param geneFilter    the list of genes we would like to study
     * @param variantFilter it is an object that gathers some features we could specify in order to reduce the scope of the result
     * @param optionsFilter we could choice to study reactions, proteic complexes or both
     * @return the method returns a list with two posible lists inside: A list of variants obtained from the complex pathway or a list
     * of variants obtained from the complex pathway
     * @throws ExecutionException   when trouble with multithreading
     * @throws InterruptedException when trouble with multithreading
     */
    public VariantContainer execute(FamilyFilter familyFilter, GeneFilter geneFilter, VariantFilter variantFilter,
                                    OptionsFilter optionsFilter) throws ExecutionException, InterruptedException {
        // FamilyFilter input
        Pedigree pedigree = familyFilter.getPedigree();
        Phenotype phenotype = familyFilter.getPhenotype();
        String moi = "";

        // FamilyFilter output
        Map<String, List<String>> genotypes;

        // GeneFilter
        List<String> geneList;

        // VariantFilter
        List<String> diseaseList;
        List<String> populationFrequencySpecies;
        double populationFrequency;
        List<String> consequenceType;

        // F A M I L Y - F I L T E R
        if (!familyFilter.getMoi().isEmpty()) {
            moi = familyFilter.getMoi();
        }
        switch (moi) {
            case "dominant":
                genotypes = ModeOfInheritance.dominant(pedigree, phenotype, false);
                break;
            case "recessive":
                genotypes = ModeOfInheritance.recessive(pedigree, phenotype, false);
                break;
            case "xlinkeddominant":
                genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, true);
                break;
            case "xlinkedrecessive":
                genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, false);
                break;
            case "ylinked":
                genotypes = ModeOfInheritance.yLinked(pedigree, phenotype);
                break;
            default:
                genotypes = new HashMap<>();
                genotypes.put(pedigree.getProband().getId(), Collections.singletonList("NON_REF"));
                break;
        }
        // yLinked or other mistakes can return empty genotype lists. The next exception aims to avoid those errors.
        genotypes.entrySet().removeIf((entry) -> CollectionUtils.isEmpty(entry.getValue()));
        if (genotypes.size() == 0) {
            throw new IllegalArgumentException("Number of individuals with filled genotypes list is zero");
        }

        // G E N E - F I L T E R
        if (CollectionUtils.isEmpty(geneFilter.getGenes()) && CollectionUtils.isEmpty(geneFilter.getPanels())
                && CollectionUtils.isEmpty(geneFilter.getDiseases())) {
            throw new IllegalArgumentException("GeneFilter cannot be empty");
        }
        Set<String> geneSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(geneFilter.getGenes())) {
            geneSet.addAll(geneFilter.getGenes());
        }
        if (CollectionUtils.isNotEmpty(geneFilter.getPanels())) {
            geneSet.addAll(panelToList(geneFilter.getPanels()));
        }
        if (CollectionUtils.isNotEmpty(geneFilter.getDiseases())) {
            geneSet.addAll(diseaseToList(geneFilter.getDiseases()));
        }
        geneList = new LinkedList<>(geneSet);

        // V A R I A N T - F I L T E R
        if (CollectionUtils.isNotEmpty(variantFilter.getDiseaseList())) {
            diseaseList = variantFilter.getDiseaseList();
        } else {
            diseaseList = Collections.emptyList();
        }

        if (CollectionUtils.isNotEmpty(variantFilter.getPopulationFrequencySpecies()) && variantFilter.getPopulationFrequency() > 0) {
            populationFrequencySpecies = variantFilter.getPopulationFrequencySpecies();
            populationFrequency = variantFilter.getPopulationFrequency();
        } else {
            populationFrequencySpecies = Collections.emptyList();
            populationFrequency = -1;
        }

        if (CollectionUtils.isNotEmpty(variantFilter.getConsequenceType())) {
            consequenceType = variantFilter.getConsequenceType();
        } else {
            consequenceType = Collections.emptyList();
        }

        // M U L T I T H R E A D E D - X - Q U E R Y
        VariantContainer variantList;
        int numThreads = Math.min(4, geneList.size());

        if (numThreads == 1) {
            variantList = xQueryCall(genotypes, geneList, diseaseList, populationFrequencySpecies, populationFrequency,
                    consequenceType, optionsFilter);
        } else {
            List<Future<VariantContainer>> futures = new ArrayList<>();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            for (String gene : geneList) {
                futures.add(executor.submit(new Threadpool(this, genotypes, gene, diseaseList, populationFrequencySpecies,
                        populationFrequency, consequenceType, optionsFilter)));
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            variantList = new VariantContainer();
            for (Future<VariantContainer> future : futures) {
                VariantContainer container = future.get();
                if (CollectionUtils.isNotEmpty(container.getComplexVariantList())) {
                    variantList.getComplexVariantList().addAll(container.getComplexVariantList());
                }
                if (CollectionUtils.isNotEmpty(container.getReactionVariantList())) {
                    variantList.getReactionVariantList().addAll(container.getReactionVariantList());
                }
            }
        }
        return variantList;
    }

    /**
     * This method gathers the processed info by XQuery and calls the Craftsman. Acts like the intermediary.
     *
     * @param genotypes                  It's a map on which we store the individuals we want to analyze related with their genotypes
     * @param geneList                   the list of genes in we would like to look for proteins
     * @param diseaseList                An optional list of diseases we would like to focuse on
     * @param populationFrequencySpecies An optional filter aimed to filter by species. Must be used jointly with "populationFrequency"
     * @param populationFrequency        An optional filter aimed to filter by the amount of people who carries a specific mutation in the
     *                                   target protein. Must be used jointly with "populationFrequency"
     * @param consequenceType            An optional filter aimed to filter by consequence type of the target protein
     * @param optionsFilter              we could choice to study reactions, proteic complexes or both
     * @return an object containing two lists corresponding to variants in proteins related to a complex and variants related to a reaction
     */
    VariantContainer xQueryCall(Map<String, List<String>> genotypes, List<String> geneList,
                                List<String> diseaseList, List<String> populationFrequencySpecies,
                                double populationFrequency, List<String> consequenceType,
                                OptionsFilter optionsFilter) {
        if (optionsFilter.isOnlyComplex() && optionsFilter.isOnlyReaction()) {
            throw new IllegalArgumentException("You can't choose both onlyReactions and onlyComplex");
        } else {
            // Booleans are both "false" by default, which means we will give both COMPLEX and REACTION nexus by default
            // This changes when the user specifies he wants "onlyComplex" or "onlyReaction"
            List<Variant> complexVariantList = Collections.emptyList();
            if (!optionsFilter.isOnlyReaction()) {
                complexVariantList = xQueryCraftsman(genotypes, geneList, diseaseList, populationFrequencySpecies,
                        populationFrequency, consequenceType, true);
            }

            List<Variant> reactionVariantList = Collections.emptyList();
            if (!optionsFilter.isOnlyComplex()) {
                reactionVariantList = xQueryCraftsman(genotypes, geneList, diseaseList, populationFrequencySpecies,
                        populationFrequency, consequenceType, false);
            }
            return new VariantContainer(complexVariantList, reactionVariantList);
        }
    }

    /**
     * This method transforms gene panels into a list of genes.
     *
     * @param panels the list of panels we would like to analyze
     * @return list of genes
     */
    private Set<String> panelToList(List<DiseasePanel> panels) {
        List<String> panelSet = new ArrayList<>();
        Set<String> geneSet = new HashSet<>();
        for (DiseasePanel panel : panels) {
            panelSet.add(panel.getName());
        }

        Session session = this.driver.session();
        StatementResult result = session.run("MATCH (panel:PANEL)-[:PANEL__GENE]-(gene:GENE) "
                + " WHERE " + getGenericSubstring(panelSet, "panel.name", false)
                + " RETURN DISTINCT gene.name AS gene");
        session.close();
        while (result.hasNext()) {
            Record record = result.next();
            geneSet.add(record.get("gene").asString());
        }
        return geneSet;
    }

    /**
     * This method transforms diseases into a list of genes.
     *
     * @param diseases the list of panels we would like to analyze
     * @return list of genes
     */
    private Set<String> diseaseToList(List<String> diseases) {
        Set<String> geneSet = new HashSet<>();
        Session session = this.driver.session();
        StatementResult result = session.run("MATCH (dis:DISEASE)-[:GENE__DISEASE]-(gene:GENE) "
                + " WHERE " + getGenericSubstring(diseases, "dis.name", false)
                + " RETURN DISTINCT gene.name AS gene");
        session.close();
        while (result.hasNext()) {
            Record record = result.next();
            geneSet.add(record.get("gene").asString());
        }
        return geneSet;
    }

    /**
     * This method builds the XQuery and calls Neo4J.
     *
     * @param genotypes                  It's a map on which we store the individuals we want to analyze related with their genotypes
     * @param geneList                   the list of genes in we would like to look for proteins
     * @param diseaseList                An optional list of diseases we would like to focuse on
     * @param populationFrequencySpecies An optional filter aimed to filter by species. Must be used jointly with "populationFrequency"
     * @param populationFrequency        An optional filter aimed to filter by the amount of people who carries a specific mutation in the
     *                                   target protein. Must be used jointly with "populationFrequency"
     * @param consequenceType            An optional filter aimed to filter by consequence type of the target protein
     * @param complexOrReaction          this boolean specifies if we want to study proteic reactions (false), or proteic complexes (true)
     * @return A list of variants either obtained from the complex pathway or the reaction pathway
     */
    private List<Variant> xQueryCraftsman(Map<String, List<String>> genotypes, List<String> geneList, List<String> diseaseList,
                                          List<String> populationFrequencySpecies, double populationFrequency, List<String> consequenceType,
                                          boolean complexOrReaction) {
        String queryString;
        // H E A D - Implements the part of the query who treats samples and diseaseList
        // Filtering by diseases is faster because of the exclusion of genes without disease related
        // IMPORTANT: the filter order is important, 1) diseases, 2) family, 3) SO/Pop. frequency
        String familyIndex = getFamilySubstrings(genotypes, geneList, false, true).get(1);

        if (CollectionUtils.isNotEmpty(diseaseList)) {
            queryString = "MATCH (dis:DISEASE)-[:GENE__DISEASE]-(gene2:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)-"
                    + "[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-";
            if (complexOrReaction) {
                queryString += "[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-";
            } else {
                queryString += "[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-";
            }
            queryString += "(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(gene:GENE)-[:XREF]-(ref:XREF)"
                    + " WHERE " + getGenericSubstring(geneList, "ref.id", true)
                    + getGenericSubstring(diseaseList, "dis.name", false)
                    + " WITH DISTINCT tr1, prot1.name AS " + NodeBuilder.TARGET_PROTEIN + ", nex.name AS " + NodeBuilder.NEXUS
                    + ", prot2.name AS " + NodeBuilder.PANEL_PROTEIN + ", ref.id AS " + NodeBuilder.PANEL_GENE + "\n"
                    + " MATCH (tr1:TRANSCRIPT)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-"
                    + "(var:VARIANT)-[:VARIANT__VARIANT_CALL]-(vc:VARIANT_CALL)-[:SAMPLE__VARIANT_CALL]-(sam:SAMPLE) ";
            if ((CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0)
                    || CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += getFamilySubstrings(genotypes, geneList, false, false).get(0);
            } else {
                queryString += getFamilySubstrings(genotypes, geneList, true, false).get(0);
            }
        } else {
            queryString = "MATCH (sam:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(var:VARIANT)-"
                    + "[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(tr1:TRANSCRIPT)-"
                    + "[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-";
            if (complexOrReaction) {
                queryString += "[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-";
            } else {
                queryString += "[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-";
            }
            queryString += "(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(gene:GENE)-[:XREF]-(ref:XREF) ";
            if ((CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0)
                    || CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += getFamilySubstrings(genotypes, geneList, false, true).get(0);
            } else {
                queryString += getFamilySubstrings(genotypes, geneList, true, true).get(0);
            }
        }

        // T A I L - Implements the part of the query that deals with ConsequenceType and PopulationFrequency
        if (CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0) {
            queryString += " MATCH (var)-[:VARIANT__POPULATION_FREQUENCY]-(pf:POPULATION_FREQUENCY)"
                    + " WHERE " + getGenericSubstring(populationFrequencySpecies, "pf.id", true)
                    + "toFloat(pf.attr_altAlleleFreq)<" + populationFrequency;
            if (CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += " WITH DISTINCT " + familyIndex + " var, " + NodeBuilder.TARGET_PROTEIN + ", " + NodeBuilder.NEXUS + ", "
                        + NodeBuilder.PANEL_PROTEIN + ", " + NodeBuilder.PANEL_GENE + "\n";
            } else {
                queryString += " RETURN DISTINCT " + familyIndex + " var.attr_chromosome AS " + NodeBuilder.CHROMOSOME
                        + ", var.attr_start AS " + NodeBuilder.START + ", var.attr_reference AS " + NodeBuilder.REFERENCE
                        + ", var.attr_alternate AS " + NodeBuilder.ALTERNATE + ", var.attr_type AS " + NodeBuilder.TYPE + ", "
                        + NodeBuilder.TARGET_PROTEIN + ", " + NodeBuilder.NEXUS + ", " + NodeBuilder.PANEL_PROTEIN + ", "
                        + NodeBuilder.PANEL_GENE;
            }
        }
        if (CollectionUtils.isNotEmpty(consequenceType)) {
            queryString += " MATCH (var)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__SO]-(so:SO)"
                    + " WHERE " + getGenericSubstring(consequenceType, "so.name", false)
                    + " RETURN DISTINCT " + familyIndex + " var.attr_chromosome AS " + NodeBuilder.CHROMOSOME + ", var.attr_start AS "
                    + NodeBuilder.START + ", var.attr_reference AS " + NodeBuilder.REFERENCE + ", var.attr_alternate AS "
                    + NodeBuilder.ALTERNATE + ", var.attr_type AS " + NodeBuilder.TYPE + ", so.name AS CT, " + NodeBuilder.TARGET_PROTEIN
                    + ", " + NodeBuilder.NEXUS + ", " + NodeBuilder.PANEL_PROTEIN + ", " + NodeBuilder.PANEL_GENE;
        }
        System.out.println(queryString);
        Session session = this.driver.session();
        StatementResult result = session.run(queryString);
        session.close();

        List<Variant> variants = new ArrayList<>();
        while (result.hasNext()) {
//            Record record = result.next();
            variants.add(new Neo4JVariantIterator(result).next());
        }
        return variants;
    }

    /**
     * This method builds the body of the sample and genotype filter.
     */
    private static List<String> getFamilySubstrings(Map<String, List<String>> genotype, List<String> geneList, boolean returnTime,
                                                    boolean absenceOfDiseaseList) {
        String familySubString;
        String familyIndex = "";
        int numberOfIndividuals = genotype.size();
        if (numberOfIndividuals == 0) {
            throw new IllegalArgumentException("Number of individuals must be at least one.");
        } else {
            int counter = 0;
            List<String> filters = new ArrayList<>();
            // Linked List better??
            for (String member : genotype.keySet()) {
                String filter = "";
                // "If there is no DISEASE filter and it is the first individual indluded."
                if (absenceOfDiseaseList && counter == 0) {
                    filter += " WHERE " + getGenericSubstring(geneList, "ref.id", true)
                            + getGenericSubstring(Collections.singletonList(member), "sam.id", true);
                    if (genotype.get(member).get(0).equals("NON_REF")) {
                        filter += " (vc.attr_GT<>'0/0') ";
                    } else {
                        filter += getGenericSubstring(genotype.get(member), "vc.attr_GT", false);
                    }
                } else {
                    filter += " WHERE " + getGenericSubstring(Collections.singletonList(member), "sam.id", true);
                    if (genotype.get(member).get(0).equals("NON_REF")) {
                        filter += " (vc.attr_GT<>'0/0') ";
                    } else {
                        filter += getGenericSubstring(genotype.get(member), "vc.attr_GT", false);
                    }
                }
                // "If there is no PF filter or CT filter and there are no more individuals left to include, Return."
                if (returnTime && counter == numberOfIndividuals - 1) {
                    filter += " RETURN DISTINCT sam.id AS SAMPLE" + counter + ", vc.attr_GT AS GENOTYPE" + counter + ", ";
                } else {
                    filter += " WITH DISTINCT sam.id AS SAMPLE" + counter + ", vc.attr_GT AS GENOTYPE" + counter + ", ";
                }
                familyIndex = "";
                for (int i = counter; i > 0; i--) {
                    familyIndex += "SAMPLE" + (i - 1) + ", GENOTYPE" + (i - 1) + ", ";
                }
                // "If there is no DISEASE filter and it is the first individual indluded."
                if (absenceOfDiseaseList && counter == 0) {
                    // "If there is no PF filter or CT filter and there are no more individuals left to include, Return."
                    if (returnTime && counter == numberOfIndividuals - 1) {
                        filter += familyIndex + "var.attr_chromosome AS " + NodeBuilder.CHROMOSOME + ", var.attr_start AS "
                                + NodeBuilder.START + ", var.attr_reference AS " + NodeBuilder.REFERENCE + ", var.attr_alternate AS "
                                + NodeBuilder.ALTERNATE + ", prot1.name AS " + NodeBuilder.TARGET_PROTEIN + ", nex.name AS "
                                + NodeBuilder.NEXUS + ", prot2.name AS " + NodeBuilder.PANEL_PROTEIN + "," + " ref.id AS "
                                + NodeBuilder.PANEL_GENE + "\n";
                    } else {
                        filter += familyIndex + "var, prot1.name AS " + NodeBuilder.TARGET_PROTEIN + ", nex.name AS " + NodeBuilder.NEXUS
                                + ", prot2.name AS " + NodeBuilder.PANEL_PROTEIN + "," + " ref.id AS " + NodeBuilder.PANEL_GENE + "\n";
                    }
                } else {
                    // "If there is no PF filter or CT filter and there are no more individuals left to include, Return."
                    if (returnTime && counter == numberOfIndividuals - 1) {
                        filter += familyIndex + "var.attr_chromosome AS " + NodeBuilder.CHROMOSOME + ", var.attr_start AS "
                                + NodeBuilder.START + ", var.attr_reference AS " + NodeBuilder.REFERENCE + ", var.attr_alternate AS "
                                + NodeBuilder.ALTERNATE + ", " + NodeBuilder.TARGET_PROTEIN + ", " + NodeBuilder.NEXUS + ", "
                                + NodeBuilder.PANEL_PROTEIN + ", " + NodeBuilder.PANEL_GENE + "\n";
                    } else {
                        filter += familyIndex + "var, " + NodeBuilder.TARGET_PROTEIN + ", " + NodeBuilder.NEXUS + ", "
                                + NodeBuilder.PANEL_PROTEIN + ", " + NodeBuilder.PANEL_GENE + "\n";
                    }
                }
                filters.add(filter);
                counter++;
            }
            familyIndex = "SAMPLE" + (numberOfIndividuals - 1) + ", GENOTYPE" + (numberOfIndividuals - 1) + ", " + familyIndex;
            familySubString = StringUtils.join(filters, " MATCH (sam:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-"
                    + "[:VARIANT__VARIANT_CALL]-(var:VARIANT) ");
            return Arrays.asList(familySubString, familyIndex);
        }
    }

    /**
     * Builds the part of the cypher query aimed to act as a searching filter. We can fiter by the individual samples, their
     * genotype, the chromosome or the genes in which we want to look up.
     *
     * @param stringList The list of elements that will compound the filter
     * @param calling    The index we want to use to call if from the database
     * @param isNotLast  A boolean that adds an "AND" operator at the end of the substring if needed
     * @return the substring with the filter ready to use for Neo4j
     */
    private static String getGenericSubstring(List<String> stringList, String calling, boolean isNotLast) {
        String substring = "";
        if (CollectionUtils.isEmpty(stringList)) {
            throw new IllegalArgumentException("getGenericString is receiving an empty list");
        }
        List<String> elements = new ArrayList<>();
        for (String element : stringList) {
            elements.add(substring + calling + "='" + element + "'");
        }
        substring = StringUtils.join(elements, " OR ");
        substring = "(" + substring + ")";
        if (isNotLast) {
            substring = substring + " AND ";
        }
        return substring;
    }
}
