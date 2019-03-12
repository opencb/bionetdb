package org.opencb.bionetdb.core.neo4j.interpretation;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.*;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.variant.Variant;

import org.opencb.bionetdb.core.neo4j.Neo4JVariantIterator;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Tiering {

    private Driver driver;

//    public static final String PREFIX_ATTRIBUTES = "attr_";

    public Tiering(Driver driver) {
        this.driver = driver;
    }

    /**
     * This method looks for all the variants of interest for a given pedigree. It should be aimed to a gene panel, and may be filtered
     * by chromosome too.
     *
     * @param listOfGenes      list of strings with the genes we want to check (may be null but shouldn't).
     * @param listOfChromosome list of strings with the genes we want to check (may be null).
     * @param individualsGT    list of strings with the genes we want to check (can't be null).
     * @return a QueryResult object containing the variants matching the specifications.
     */
    public QueryResult<Variant> getVariantsFromPedigree(List<String> listOfGenes, List<String> listOfChromosome,
                                                        Map<String, List<String>> individualsGT) {

        String queryString;
        List<String> matches = new ArrayList<>();
        List<String> individualsID = new ArrayList<>(individualsGT.keySet());
        String genesSubstring = getGenericSubstring(listOfGenes, "ref.id", true);
        String chromosomeSubstring = getGenericSubstring(listOfChromosome, "var.attr_chromosome", true);

        for (String individual : individualsID) {
            String genotypeSubstring = getGenericSubstring(individualsGT.get(individual), "vc.attr_GT", true);
            queryString = "MATCH (sam:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(var:VARIANT)"
                    + "-[:VARIANT__CONSEQUENCE_TYPE]-(:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(:TRANSCRIPT)-[:GENE__TRANSCRIPT]"
                    + "-(:GENE)-[:XREF]-(ref:XREF)"
                    + " WHERE " + genesSubstring + chromosomeSubstring + genotypeSubstring + "sam.id='" + individual + "'";
            matches.add(queryString);
        }
        queryString = StringUtils.join(matches, " WITH DISTINCT var\n");
        queryString = queryString + " RETURN DISTINCT var.name";
        System.out.println("queryString = " + queryString + "\n");

        return getVariantsFromNeo(queryString);
    }

//  DE AQUÍ PARRIBA SON PA DOMINANT, RECESSIVE Y LINKED ///////////////////////////////////////////////////////
//  ///////////////////////////////////////////////////////////////////// DE AQUÍ PABAJO SON PAL CH Y EL DENOVO

    /**
     * This method looks for all the variants pertaining to a set of individuals. It should be aimed to a gene panel, and may be filtered
     * by chromosome too.
     *
     * @param listOfGenes      list of strings with the genes we want to check (may be null but shouldn't).
     * @param listOfChromosome list of strings with the genes we want to check (may be null).
     * @param listOfMembers    list of strings with the genes we want to check (can't be null).
     * @return a StatementResult object containing the variants matching the specifications.
     */
    public Neo4JVariantIterator variantsToIterator(List<String> listOfGenes, List<String> listOfChromosome,
                                                   List<Member> listOfMembers) {

        // It's likely that this action could be done more efficiently.
        List<String> stringListOfIndividuals = new LinkedList<>();
        for (Member member : listOfMembers) {
            stringListOfIndividuals.add(member.getId());
        }

        // Maybe we could change the filtering by chromosome to the genes instead of the variants. It's more efficient.
        // For that we should modify the method "getGenericSubstring". También habría que apañar la entrada por paneles o PH.
        String individualsSubstring = getGenericSubstring(stringListOfIndividuals, "sam.id", false);
        String genesSubstring = getGenericSubstring(listOfGenes, "ref.id", true);
        String chromosomeSubstring = getGenericSubstring(listOfChromosome, "var.attr_chromosome", true);
        String matchString = "MATCH (sam:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(var:VARIANT)"
                + "-[:VARIANT__CONSEQUENCE_TYPE]-(:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(:TRANSCRIPT)-[:GENE__TRANSCRIPT]"
                + "-(gene:GENE)-[:XREF]-(ref:XREF)"
                + " WHERE " + genesSubstring + chromosomeSubstring + individualsSubstring
                + " WITH var, collect(DISTINCT sam.id) AS sam_collection, collect(vc.attr_GT) AS gt_collection,"
                + " COUNT (DISTINCT sam.id) AS num_of_sam\n";
        String returnString = " RETURN var.attr_chromosome AS " + NodeBuilder.CHROMOSOME
                + ", var.attr_start AS " + NodeBuilder.START
                + ", var.attr_end AS " + NodeBuilder.END
                + ", var.attr_reference AS " + NodeBuilder.REFERENCE
                + ", var.attr_alternate AS " + NodeBuilder.ALTERNATE
                + ", var.attr_type AS " + NodeBuilder.TYPE
                + ", sam_collection, gt_collection[0..num_of_sam] AS gt_collection, num_of_sam";
        String queryString = matchString + returnString;
        System.out.println("queryString = " + queryString + "\n");

        Session session = this.driver.session();
        StatementResult result = session.run(queryString);
        session.close();
        return new Neo4JVariantIterator(result);
    }

    /**
     * This method gets a list of variants from the database. It is only useful if we want different information than the one
     * given by the list we use as argument, otherwise neo4j result will be redundant
     *
     * @param listOfVariants the list of variants we want to get from neo4j database
     * @return a QueryResult object containing the variants in the list.
     */
    public QueryResult<Variant> getVariantsFromList(List<Variant> listOfVariants) {
        String startingString = "MATCH (var:VARIANT) WHERE (var.name='";
        String variantsString = StringUtils.join(listOfVariants, "' OR var.name='") + "')";
        String endingString = " RETURN var.name";
        String queryString = startingString + variantsString + endingString;
        return getVariantsFromNeo(queryString);
    }

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////// THIS METHODS ARE AUXILIAR FOR THE ONES ABOVE ////////////////////////////////////

    /**
     * Builds the part of the cypher query aimed to act as a searching filter. We can fiter by the individual samples, their
     * genotype, the chromosome or the genes in which we want to look up.
     * <p>
     * [Mainly used for methods "getVariantsFromPedigree" and "getVariantsFromList"]
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
     * This method calls Neo4j driver, executes the query and returns a list of variants inside a queryObject.
     * <p>
     * [Mainly used for methods "getVariantsFromPedigree" and "variantsToIterator"]
     *
     * @param queryString The cypher query we wish to execute in Neo4j database
     * @return QueryOption object containing a List of variants as a result of the query
     */
    private QueryResult<Variant> getVariantsFromNeo(String queryString) {
        Session session = this.driver.session();
        StatementResult result = session.run(queryString);
        session.close();
        List<Variant> variants = new ArrayList<>();
        while (result.hasNext()) {
            Record record = result.next();
            variants.add(new Variant(record.get("var.name").asString()));
        }
        QueryResult<Variant> variantsResult = new QueryResult<>("variants");
        variantsResult.setResult(variants);
        return variantsResult;
    }
}
