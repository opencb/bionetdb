package org.opencb.bionetdb.core.neo4j.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;

import java.util.*;

public class XQueryAnalysis {

    public void xQuery(Pedigree pedigree, Phenotype phenotype, String moi, List<String> listOfGenes, List<String> listOfDiseases,
                       List<String> populationFrequencySpecies, double populationFrequency, List<String> consequenceType,
                       boolean onlyComplex, boolean onlyReaction) {
        Map<String, List<String>> GT;
        // Try Catch por si ponen Dominant o dOMINANT o cosas de esas
        switch (moi) {
            case "dominant":
                GT = ModeOfInheritance.dominant(pedigree, phenotype, false);
                break;
            case "recessive":
                GT = ModeOfInheritance.recessive(pedigree, phenotype, false);
                break;
            default:
                GT = ModeOfInheritance.recessive(pedigree, phenotype, false);
                break;
        }

        // Los booleanos son ambos "false" por defecto, lo que significa que por defecto pasamos tanto COMPLEX como REACTION.
        // Ésto cambia si el usuario especifica que quiere "onlyComplex" o "onlyReaction"
        if (!onlyReaction) {
            xQueryCraftsman(GT, listOfGenes, listOfDiseases, populationFrequencySpecies, populationFrequency,
                    consequenceType, true);
        }
        if (!onlyComplex) {
            xQueryCraftsman(GT, listOfGenes, listOfDiseases, populationFrequencySpecies, populationFrequency,
                    consequenceType, false);
        }
    }

    // Si se mantiene público es recomendable pasar la entrada de GT a ista de individuos y genotipos, más estándar.
    public void xQueryCraftsman(Map<String, List<String>> GT, List<String> listOfGenes, List<String> listOfDiseases,
                                List<String> populationFrequencySpecies, double populationFrequency, List<String> consequenceType,
                                boolean complexOrReaction) {
        // Population frecuency podemos ponerla invertida (1 - 0.99) pa q sea más sencillo.
        String queryString;
        // HEAD
        // Filtering by diseases is faster because of the exclusion of genes without disease related
        String familyIndex = getFamilySubstrings(GT, listOfGenes, GT.size(), false, true).get(1);

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
                queryString += getFamilySubstrings(GT, listOfGenes, GT.size(), false, false).get(0);
            } else {
                queryString += getFamilySubstrings(GT, listOfGenes, GT.size(), true, false).get(0);
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
                queryString += getFamilySubstrings(GT, listOfGenes, GT.size(), false, true).get(0);
            } else {
                queryString += getFamilySubstrings(GT, listOfGenes, GT.size(), true, true).get(0);
            }
        }

        // TAIL
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

    private static List<String> getFamilySubstrings(Map<String, List<String>> GT, List<String> listOfGenes, int numOfInd,
                                                    boolean returnTime, boolean listOfDiseasesAbsence) {
        String familySubString = "";
        String familyIndex = "";
        if (numOfInd == 0) {
            // Lanzar excepción pq no puede haber cero individuos
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
                            getGenericSubstring(Collections.singletonList(member), "sam.id", true) +
                            getGenericSubstring(GT.get(member), "vc.attr_GT", false);
                } else {
                    filter += " WHERE " + getGenericSubstring(Collections.singletonList(member), "sam.id", true) +
                            getGenericSubstring(GT.get(member), "vc.attr_GT", false);
                }
                if (returnTime && counter == numOfInd - 1) {
                    filter += " RETURN sam.id AS SAMPLE" + counter + ", vc.attr_GT AS GENOTYPE" + counter + ", ";
                } else {
                    filter += " WITH DISTINCT sam.id AS SAMPLE" + counter + ", vc.attr_GT AS GENOTYPE" + counter + ", ";
                }
                familyIndex = "";
                for (int i = counter; i > 0; i--) {
                    familyIndex += "SAMPLE" + (i - 1) + ", GENOTYPE" + (i - 1) + ", ";
                }
                if (listOfDiseasesAbsence && counter == 0) {
                    filter += familyIndex + "var, prot1.name AS MUT_PROT, nex.name AS NEXUS, prot2.name AS PANEL_PROT, ref.id AS GENE \n";
                } else {
                    filter += familyIndex + "var, MUT_PROT, NEXUS, PANEL_PROT, GENE \n";
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
