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

import java.util.*;

public class XQueryAnalysis {

    private Driver driver;

    private List<List<Variant>> listOfVariants = new ArrayList<>();

    public XQueryAnalysis(Driver driver) {
        this.driver = driver;
    }

    public List<List<Variant>> xQuery(FamilyFilter familyFilter, GeneFilter geneFilter) {
        VariantFilter variantFilter = new VariantFilter();
        OptionsFilter optionsFilter = new OptionsFilter();
        return xQuery(familyFilter, geneFilter, variantFilter, optionsFilter);
    }

    public List<List<Variant>> xQuery(FamilyFilter familyFilter, GeneFilter geneFilter, OptionsFilter optionsFilter) {
        VariantFilter variantFilter = new VariantFilter();
        return xQuery(familyFilter, geneFilter, variantFilter, optionsFilter);
    }

    public List<List<Variant>> xQuery(FamilyFilter familyFilter, GeneFilter geneFilter, VariantFilter variantFilter) {
        OptionsFilter optionsFilter = new OptionsFilter();
        return xQuery(familyFilter, geneFilter, variantFilter, optionsFilter);
    }

    /**
     * This method acts as a manager for XQueryCraftsman. It takes the arguments with the OpenCGA standard and converts them in
     * simpler forms that allow XQueryCraftsman to create the XQuery.
     *
     * @param familyFilter  it is an object that gathers the data of the proband and his family
     * @param geneFilter    the list of genes we would like to study
     * @param variantFilter it is an object that gathers some features we could specify in order to reduce the scope of the result
     * @param optionsFilter we could choice to study reactions, proteic complexes or both
     * @return the method returns a list with two posible lists inside: A list of variants obtained from the complex pathway or a list
     * of variants obtained from the complex pathway
     */
    public List<List<Variant>> xQuery(FamilyFilter familyFilter, GeneFilter geneFilter, VariantFilter variantFilter,
                                      OptionsFilter optionsFilter) {

        // FamilyFilter input
        Pedigree pedigree = familyFilter.getPedigree();
        Phenotype phenotype = familyFilter.getPhenotype();
        String moi = "";

        // FamilyFilter output
        Map<String, List<String>> genotypes;

        // GeneFilter
        Set<String> setOfGenes = new HashSet<>();

        // VariantFilter
        List<String> listOfDiseases;
        List<String> populationFrequencySpecies;
        double populationFrequency;
        List<String> consequenceType;

        // F A M I L Y - F I L T E R
        if (!familyFilter.getMoi().isEmpty()) {
            moi = familyFilter.getMoi();
        }
        // TTry/Catch in case they put dOMINANT or Dominant or that kind of mistakes???
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

        // G E N E - F I L T E R
        if (CollectionUtils.isEmpty(geneFilter.getGenes()) && CollectionUtils.isEmpty(geneFilter.getPanels())
                && CollectionUtils.isEmpty(geneFilter.getDiseases())) {
            throw new IllegalArgumentException("GeneFilter cannot be empty");
        }
        if (CollectionUtils.isNotEmpty(geneFilter.getGenes())) {
            setOfGenes.addAll(geneFilter.getGenes());
        }
        if (CollectionUtils.isNotEmpty(geneFilter.getPanels())) {
            setOfGenes.addAll(panelToList(geneFilter.getPanels()));
        }
        if (CollectionUtils.isNotEmpty(geneFilter.getDiseases())) {
            setOfGenes.addAll(diseaseToList(geneFilter.getDiseases()));
        }
        List<String> listOfGenes = new LinkedList<>(setOfGenes);

        if (genotypes.size() == 0) {
            throw new IllegalArgumentException("Number of individuals with filled genotypes list is zero");
        }

        // V A R I A N T - F I L T E R
        if (CollectionUtils.isNotEmpty(variantFilter.getListOfDiseases())) {
            listOfDiseases = variantFilter.getListOfDiseases();
        } else {
            listOfDiseases = Collections.emptyList();
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

        if (optionsFilter.isOnlyComplex() && optionsFilter.isOnlyReaction()) {
            throw new IllegalArgumentException("You can't chose both onlyReactions and onlyComplex");
        } else {
            // Booleans are both "false" by default, which means we will give both COMPLEX como REACTION nexus by def.
            // This changes when the user specifies he wants "onlyComplex" or "onlyReaction"
            if (!optionsFilter.isOnlyReaction()) {
                List<Variant> listOfComplexVariant = xQueryCraftsman(genotypes, listOfGenes, listOfDiseases, populationFrequencySpecies,
                        populationFrequency, consequenceType, true);
                listOfVariants.add(listOfComplexVariant);
            }
            if (!optionsFilter.isOnlyComplex()) {
                List<Variant> listOfReactionVariant = xQueryCraftsman(genotypes, listOfGenes, listOfDiseases, populationFrequencySpecies,
                        populationFrequency, consequenceType, false);
                listOfVariants.add(listOfReactionVariant);
            }
        }
        return listOfVariants;
    }

    private Set<String> panelToList(List<DiseasePanel> panels) {
        Session session = this.driver.session();
        StatementResult result = session.run("" + panels + "");
        session.close();

        Set<String> setOfGenes = new HashSet<>();
        while (result.hasNext()) {
            Record record = result.next();
            setOfGenes.add(record.get("gene.id").asString());
        }
        return setOfGenes;
    }

    private Set<String> diseaseToList(List<String> diseases) {
        Set<String> setOfGenes = new HashSet<>();
        Session session = this.driver.session();
        StatementResult result = session.run("MATCH (dis:DISEASE)-[:GENE__DISEASE]-(gene:GENE) "
                + " WHERE " + getGenericSubstring(diseases, "dis.name", false)
                + " RETURN DISTINCT gene.name AS gene");
        session.close();
        while (result.hasNext()) {
            Record record = result.next();
            setOfGenes.add(record.get("gene").asString());
        }
        return setOfGenes;
    }

    /**
     * This method builds the XQuery.
     *
     * @param genotypes                  It's a map on which we store the individuals we want to analyze related with their genotypes
     * @param listOfGenes                the list of genes in we would like to look for proteins
     * @param listOfDiseases             An optional list of diseases we would like to focuse on
     * @param populationFrequencySpecies An optional filter aimed to filter by species. Must be used jointly with "populationFrequency"
     * @param populationFrequency        An optional filter aimed to filter by the amount of people who carries a specific mutation in the
     *                                   target protein. Must be used jointly with "populationFrequency"
     * @param consequenceType            An optional filter aimed to filter by consequence type of the target protein
     * @param complexOrReaction          this boolean specifies if we want to study proteic reactions (false), or proteic complexes (true)
     * @return A list of variants either obtained from the complex pathway or the reaction pathway
     */
    public List<Variant> xQueryCraftsman(Map<String, List<String>> genotypes, List<String> listOfGenes, List<String> listOfDiseases,
                                         List<String> populationFrequencySpecies, double populationFrequency, List<String> consequenceType,
                                         boolean complexOrReaction) {
        String queryString;
        // H E A D - Implements the part of the query who treats samples and listOfDiseases
        // Filtering by diseases is faster because of the exclusion of genes without disease related
        String familyIndex = getFamilySubstrings(genotypes, listOfGenes, false, true).get(1);

        if (CollectionUtils.isNotEmpty(listOfDiseases)) {
            queryString = "MATCH (dis:DISEASE)-[:GENE__DISEASE]-(gene2:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)-"
                    + "[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-";
            if (complexOrReaction) {
                queryString += "[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-";
            } else {
                queryString += "[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-";
            }
            queryString += "(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(gene:GENE)-[:XREF]-(ref:XREF)"
                    + " WHERE " + getGenericSubstring(listOfGenes, "ref.id", true)
                    + getGenericSubstring(listOfDiseases, "dis.name", false)
                    + " WITH DISTINCT tr1, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, ref.id AS GENE\n"
                    + " MATCH (tr1:TRANSCRIPT)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-"
                    + "(var:VARIANT)-[:VARIANT__VARIANT_CALL]-(vc:VARIANT_CALL)-[:SAMPLE__VARIANT_CALL]-(sam:SAMPLE) ";
            if ((CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0)
                    || CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += getFamilySubstrings(genotypes, listOfGenes, false, false).get(0);
            } else {
                queryString += getFamilySubstrings(genotypes, listOfGenes, true, false).get(0);
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
                queryString += getFamilySubstrings(genotypes, listOfGenes, false, true).get(0);
            } else {
                queryString += getFamilySubstrings(genotypes, listOfGenes, true, true).get(0);
            }
        }

        // T A I L - Implements the part of the query who treats ConsequenceType and PopulationFrequency
        if (CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0) {
            queryString += " MATCH (var)-[:VARIANT__POPULATION_FREQUENCY]-(pf:POPULATION_FREQUENCY)"
                    + " WHERE " + getGenericSubstring(populationFrequencySpecies, "pf.id", true)
                    + "toFloat(pf.attr_altAlleleFreq)<" + populationFrequency;
            if (CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += " WITH DISTINCT " + familyIndex + " var, MUT_PROT, NEXUS, PANEL_PROT, GENE\n";
            } else {
                queryString += " RETURN DISTINCT " + familyIndex + " var.name AS VARIANT, MUT_PROT, NEXUS, PANEL_PROT, GENE";
            }
        }
        if (CollectionUtils.isNotEmpty(consequenceType)) {
            queryString += " MATCH (var)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__SO]-(so:SO)"
                    + " WHERE " + getGenericSubstring(consequenceType, "so.name", false)
                    + " RETURN DISTINCT " + familyIndex + " var.name AS VARIANT, so.name AS CT, MUT_PROT, NEXUS, PANEL_PROT, GENE";

        }
//        System.out.println(queryString);
        Session session = this.driver.session();
        StatementResult result = session.run(queryString);
        session.close();

        List<Variant> variants = new ArrayList<>();
        while (result.hasNext()) {
            Record record = result.next();
            variants.add(new Variant(record.get("VARIANT").asString()));
        }
        return variants;
    }

    /**
     * This method builds the body of the sample and genotype filter.
     */
    private static List<String> getFamilySubstrings(Map<String, List<String>> genotype, List<String> listOfGenes, boolean returnTime,
                                                    boolean listOfDiseasesAbsence) {
        String familySubString;
        String familyIndex = "";
        int numOfInd = genotype.size();
        if (numOfInd == 0) {
            throw new IllegalArgumentException("Number of individuals must be at least one.");
        } else {
            int counter = 0;
            List<String> filters = new ArrayList<>();
            // Linked List better??
            for (String member : genotype.keySet()) {
                String filter = "";
                if (listOfDiseasesAbsence && counter == 0) {
                    filter += " WHERE " + getGenericSubstring(listOfGenes, "ref.id", true)
                            + getGenericSubstring(Collections.singletonList(member), "sam.id", true);
                    if (genotype.get(member).get(0).equals("NON_REF")) {
                        // This could be also implemented in method getGenericString if needed.
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
                if (returnTime && counter == numOfInd - 1) {
                    filter += " RETURN DISTINCT sam.id AS SAMPLE" + counter + ", vc.attr_GT AS GENOTYPE" + counter + ", ";
                } else {
                    filter += " WITH DISTINCT sam.id AS SAMPLE" + counter + ", vc.attr_GT AS GENOTYPE" + counter + ", ";
                }
                familyIndex = "";
                for (int i = counter; i > 0; i--) {
                    familyIndex += "SAMPLE" + (i - 1) + ", GENOTYPE" + (i - 1) + ", ";
                }
                if (listOfDiseasesAbsence && counter == 0) {
                    if (returnTime && counter == numOfInd - 1) {
                        filter += familyIndex + "var.name AS VARIANT, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, "
                                + "ref.id AS GENE \n";
                    } else {
                        filter += familyIndex + "var, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, "
                                + "ref.id AS GENE \n";
                    }
                } else {
                    if (returnTime && counter == numOfInd - 1) {
                        filter += familyIndex + "var.name AS VARIANT, MUT_PROT, NEXUS, PANEL_PROT, GENE \n";
                    } else {
                        filter += familyIndex + "var, MUT_PROT, NEXUS, PANEL_PROT, GENE \n";
                    }
                }
                filters.add(filter);
                counter++;
            }
            familyIndex = "SAMPLE" + (numOfInd - 1) + ", GENOTYPE" + (numOfInd - 1) + ", " + familyIndex;
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

