package org.opencb.bionetdb.core.neo4j.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;

import java.util.*;

public class XQueryAnalysis {

    private Driver driver;

    public XQueryAnalysis(Driver driver) {
        this.driver = driver;
    }

    public void xQuery(FamilyFilter familyFilter, List<String> listOfGenes) {
        VariantFilter variantFilter = new VariantFilter();
        Options options = new Options();
        xQuery(familyFilter, listOfGenes, variantFilter, options);
    }

    public void xQuery(FamilyFilter familyFilter, List<String> listOfGenes, Options options) {
        VariantFilter variantFilter = new VariantFilter();
        xQuery(familyFilter, listOfGenes, variantFilter, options);
    }

    public void xQuery(FamilyFilter familyFilter, List<String> listOfGenes, VariantFilter variantFilter) {
        Options options = new Options();
        xQuery(familyFilter, listOfGenes, variantFilter, options);
    }

    /**
     * This method acts as a manager for XQueryCraftsman. It takes the arguments with the OpenCGA standard and converts them in
     * simpler forms that allow XQueryCraftsman to create the XQuery
     *
     * @param familyFilter  it is an object that gathers the data of the proband and his family
     * @param listOfGenes   the list of genes we would like to study
     * @param variantFilter it is an object that gathers some features we could specify in order to reduce the scope of the result
     * @param options       we could choice to study reactions, proteic complexes or both
     */
    public void xQuery(FamilyFilter familyFilter, List<String> listOfGenes, VariantFilter variantFilter,
                       Options options) {

        // MOI input
        Pedigree pedigree = familyFilter.getPedigree();
        Phenotype phenotype = familyFilter.getPhenotype();
        String moi;

        // MOI output
        Map<String, List<String>> GT;
        boolean absenceOfMOI = false;

        // GeneFilter
        List<String> listOfDiseases;
        List<String> populationFrequencySpecies;
        double populationFrequency;
        List<String> consequenceType;

        if (!familyFilter.getMoi().isEmpty()) {
            moi = familyFilter.getMoi();
        } else {
            moi = "";
        }

        // Try Catch por si ponen Dominant o dOMINANT o cosas de esas
        switch (moi) {
            case "dominant":
                GT = ModeOfInheritance.dominant(pedigree, phenotype, false);
                break;
            case "recessive":
                GT = ModeOfInheritance.recessive(pedigree, phenotype, false);
                break;
            case "xlinkeddominant":
                GT = ModeOfInheritance.xLinked(pedigree, phenotype, true);
                break;
            case "xlinkedrecessive":
                GT = ModeOfInheritance.xLinked(pedigree, phenotype, false);
                break;
            case "ylinked":
                GT = ModeOfInheritance.yLinked(pedigree, phenotype);
                break;
            default:
                List<org.opencb.biodata.models.core.pedigree.Individual> familyMembers = pedigree.getMembers();
                GT = new HashMap<>();
                for (org.opencb.biodata.models.core.pedigree.Individual member : familyMembers) {
                    GT.put(member.getId(), Collections.emptyList());
                }
                absenceOfMOI = true;
                break;
        }

        if (CollectionUtils.isNotEmpty(variantFilter.getListOfDiseases())) {
            listOfDiseases = variantFilter.getListOfDiseases();
        } else {
            listOfDiseases = Collections.emptyList();
        }

        if (CollectionUtils.isNotEmpty(variantFilter.getPopulationFrequencySpecies())) {
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

        // T H R O W - E X C E P T I O N
        if (options.isOnlyComplex() && options.isOnlyReaction()) {
            System.out.println("You can't chose both onlyReactions and onlyComplex");
        } else {
            // Los booleanos son ambos "false" por defecto, lo que significa que por defecto pasamos tanto COMPLEX como REACTION.
            // Ésto cambia si el usuario especifica que quiere "onlyComplex" o "onlyReaction"
            if (!options.isOnlyReaction()) {
                xQueryCraftsman(GT, listOfGenes, listOfDiseases, populationFrequencySpecies, populationFrequency,
                        consequenceType, true, absenceOfMOI);
            }
            if (!options.isOnlyComplex()) {
                xQueryCraftsman(GT, listOfGenes, listOfDiseases, populationFrequencySpecies, populationFrequency,
                        consequenceType, false, absenceOfMOI);
            }
        }
    }

    /**
     * This method builds the XQuery
     *
     * @param GT                         It's a map on which we store the individuals we want to analyze related with their genotypes
     * @param listOfGenes                the list of genes in we would like to look for proteins
     * @param listOfDiseases             An optional list of diseases we would like to focuse on
     * @param populationFrequencySpecies An optional filter aimed to filter by species. Must be used jointly with "populationFrequency"
     * @param populationFrequency        An optional filter aimed to filter by the amount of people who carries a specific mutation in the
     *                                   target protein. Must be used jointly with "populationFrequency"
     * @param consequenceType            An optional filter aimed to filter by consequence type of the target protein
     * @param complexOrReaction          this boolean specifies if we want to study proteic reactions (false), or proteic complexes (true)
     * @param absenceOfMOI                      We must make it true in case we don't want to specify any MOI
     */
    public void xQueryCraftsman(Map<String, List<String>> GT, List<String> listOfGenes, List<String> listOfDiseases,
                                List<String> populationFrequencySpecies, double populationFrequency, List<String> consequenceType,
                                boolean complexOrReaction, boolean absenceOfMOI) {
        // Population frecuency podemos ponerla invertida (1 - 0.99) pa q sea más sencillo.
        String queryString;
        // HEAD - Implements the part of the query who treats samples and listOfDiseases
        // Filtering by diseases is faster because of the exclusion of genes without disease related
        String familyIndex = getFamilySubstrings(GT, listOfGenes, false, true, absenceOfMOI).get(1);

        if (CollectionUtils.isNotEmpty(listOfDiseases)) {
            queryString = "MATCH (dis:DISEASE)-[:GENE__DISEASE]-(gene2:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)-" +
                    "[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-";
            if (complexOrReaction) {
                queryString += "[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-";
            } else {
                queryString += "[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-";
            }
            queryString += "(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(gene:GENE)-[:XREF]-(ref:XREF)" +
                    " WHERE " + getGenericSubstring(listOfGenes, "ref.id", true) +
                    getGenericSubstring(listOfDiseases, "dis.name", false) +
                    " WITH DISTINCT tr1, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, ref.id AS GENE\n" +
                    " MATCH (tr1:TRANSCRIPT)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-" +
                    "(var:VARIANT)-[:VARIANT__VARIANT_CALL]-(vc:VARIANT_CALL)-[:SAMPLE__VARIANT_CALL]-(sam:SAMPLE) ";
            if ((CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0) ||
                    CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += getFamilySubstrings(GT, listOfGenes, false, false, absenceOfMOI).get(0);
            } else {
                queryString += getFamilySubstrings(GT, listOfGenes, true, false, absenceOfMOI).get(0);
            }
        } else {
            queryString = "MATCH (sam:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(var:VARIANT)-" +
                    "[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(tr1:TRANSCRIPT)-" +
                    "[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-";
            if (complexOrReaction) {
                queryString += "[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-";
            } else {
                queryString += "[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-";
            }
            queryString += "(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(gene:GENE)-[:XREF]-(ref:XREF) ";
            if ((CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0) ||
                    CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += getFamilySubstrings(GT, listOfGenes, false, true, absenceOfMOI).get(0);
            } else {
                queryString += getFamilySubstrings(GT, listOfGenes, true, true, absenceOfMOI).get(0);
            }
        }

        // TAIL - Implements the part of the query who treats ConsequenceType and PopulationFrequency
        if (CollectionUtils.isNotEmpty(populationFrequencySpecies) && populationFrequency > 0) {

            queryString += " MATCH (var)-[:VARIANT__POPULATION_FREQUENCY]-(pf:POPULATION_FREQUENCY)" +
                    " WHERE " + getGenericSubstring(populationFrequencySpecies, "pf.id", true) +
                    "toFloat(pf.attr_refAlleleFreq)>" + populationFrequency;
            // Explicitar que population frecuency ha de ser un valor entre 0 y 1, usando un punto como delimitación separación
            // entre parte entera y decimal

            if (CollectionUtils.isNotEmpty(consequenceType)) {
                queryString += " WITH DISTINCT " + familyIndex + " var, pf.attr_refAlleleFreq AS PF, MUT_PROT, NEXUS," +
                        " PANEL_PROT, GENE\n" +
                        " MATCH (var)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__SO]-(so:SO)" +
                        " WHERE " + getGenericSubstring(consequenceType, "so.name", false) +
                        " RETURN DISTINCT " + familyIndex + " var.name, so.name, PF, MUT_PROT, NEXUS, PANEL_PROT, GENE";
            } else {
                queryString += " RETURN DISTINCT " + familyIndex + " var.name, pf.attr_refAlleleFreq AS PF, MUT_PROT, NEXUS," +
                        " PANEL_PROT, GENE";
            }
        } else {
            if (CollectionUtils.isNotEmpty(consequenceType)) {

                queryString += " MATCH (var)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__SO]-(so:SO)" +
                        " WHERE " + getGenericSubstring(consequenceType, "so.name", false) +
                        " RETURN DISTINCT " + familyIndex + " var.name, so.name, MUT_PROT, NEXUS, PANEL_PROT, GENE";
            }
        }
        System.out.println(queryString);
//        Session session = this.driver.session();
//        StatementResult result = session.run(queryString);
//        session.close();
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
        if (stringList.size() == 0) {
            return substring;
        } else {
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

    /**
     * This method builds the body of the sample and genotype filter
     */
    private static List<String> getFamilySubstrings(Map<String, List<String>> GT, List<String> listOfGenes, boolean returnTime,
                                                    boolean listOfDiseasesAbsence, boolean noMOI) {
        String familySubString = "";
        String familyIndex = "";
        int numOfInd = GT.size();
        if (numOfInd == 0) {
            // Throw an exception. 0 individuals is impossible.
            return Arrays.asList(familySubString, familyIndex);
        } else {
            int counter = 0;
            List<String> filters = new ArrayList<>();
            // Linked List mejor??
            for (String member : GT.keySet()) {
                // En lugar de un counter y este bucle for podría haber utilizado la forma con la "i" pero había que hacer una
                // conversión de Set a List al obtener las keys y no he preferido ésto. Tampoco cambia mucho.
                String filter = "";
                if (listOfDiseasesAbsence && counter == 0) {
                    filter += " WHERE " + getGenericSubstring(listOfGenes, "ref.id", true) +
                            getGenericSubstring(Collections.singletonList(member), "sam.id", true);
                    if (noMOI) {
                        // This could be also implemented in method getGenericString if needed.
                        filter += " (vc.attr_GT<>'0/0') ";
                    } else {
                        filter += getGenericSubstring(GT.get(member), "vc.attr_GT", false);
                    }
                } else {
                    filter += " WHERE " + getGenericSubstring(Collections.singletonList(member), "sam.id", true);
                    if (noMOI) {
                        filter += " (vc.attr_GT<>'0/0') ";
                    } else {
                        filter += getGenericSubstring(GT.get(member), "vc.attr_GT", false);
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
                        filter += familyIndex + "var.name, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, " +
                                "ref.id AS GENE \n";
                    } else {
                        filter += familyIndex + "var, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, " +
                                "ref.id AS GENE \n";
                    }
                } else {
                    if (returnTime && counter == numOfInd - 1) {
                        filter += familyIndex + "var.name, MUT_PROT, NEXUS, PANEL_PROT, GENE \n";
                    } else {
                        filter += familyIndex + "var, MUT_PROT, NEXUS, PANEL_PROT, GENE \n";
                    }
                }
                filters.add(filter);
                counter++;
            }
            familyIndex = "SAMPLE" + (numOfInd - 1) + ", GENOTYPE" + (numOfInd - 1) + ", " + familyIndex;
            familySubString = StringUtils.join(filters, " MATCH (sam:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-" +
                    "[:VARIANT__VARIANT_CALL]-(var:VARIANT) ");
            return Arrays.asList(familySubString, familyIndex);
        }
    }
}
