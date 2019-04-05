package org.opencb.bionetdb.core.neo4j.query;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.api.query.VariantQueryParam;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.bionetdb.core.utils.NodeBuilder.*;
import static org.opencb.bionetdb.core.utils.NodeBuilder.PANEL_GENE;

public class Neo4JVariantQueryParser {

    private static final Pattern POP_FREQ_PATTERN = Pattern.compile("([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");

    public static String parse(Query query, QueryOptions options) {
        // Check query
        Set<VariantQueryParam> includeMap = getIncludeMap(query);
        Set<VariantQueryParam> excludeMap = getExcludeMap(query);
        if (CollectionUtils.isNotEmpty(includeMap) && CollectionUtils.isNotEmpty(excludeMap)) {
            throw new IllegalArgumentException("Invalid query: mixing INCLUDE and EXCLUDE parameters is not permitted.");
        }

        // Include attributes
        List<String> includeAttrs = getIncludeAttributes(includeMap, excludeMap);

        // Get Cypher statements in order to build the Cypher query
        String cypher;
        if (query.containsKey(VariantQueryParam.PANEL.key()) && query.containsKey(VariantQueryParam.GENE.key())) {
            String geneValues = query.getString(VariantQueryParam.GENE.key());
            query.remove(VariantQueryParam.GENE.key());

            List<Neo4JQueryParser.CypherStatement> panelCypherStatements = getCypherStatements(query, options);

            query.remove(VariantQueryParam.PANEL.key());
            query.put(VariantQueryParam.GENE.key(), geneValues);

            List<Neo4JQueryParser.CypherStatement> geneCypherStatements = getCypherStatements(query, options);

            cypher = buildCypherStatement(query, includeAttrs, panelCypherStatements)
                    + "\nUNION\n"
                    + buildCypherStatement(query, includeAttrs, geneCypherStatements);
        } else {
            cypher = buildCypherStatement(query, includeAttrs, getCypherStatements(query, options));
        }

        return cypher;
    }

    public static String parseProteinNetworkInterpretation(Query query, QueryOptions options, boolean complexOrReaction) {
        // Check query
        Set<VariantQueryParam> includeMap = getIncludeMap(query);
        Set<VariantQueryParam> excludeMap = getExcludeMap(query);
        if (CollectionUtils.isNotEmpty(includeMap) && CollectionUtils.isNotEmpty(excludeMap)) {
            throw new IllegalArgumentException("Invalid query: mixing INCLUDE and EXCLUDE parameters is not permitted.");
        }

        // Include attributes
        List<String> includeAttrs = getIncludeAttributes(includeMap, excludeMap);

        String cypher;

        if (query.containsKey(VariantQueryParam.PANEL.key()) && query.containsKey(VariantQueryParam.GENE.key())) {
            String geneValues = query.getString(VariantQueryParam.GENE.key());
            String biotypeValues = query.getString(VariantQueryParam.ANNOT_BIOTYPE.key());
            String chromValues = query.getString(VariantQueryParam.CHROMOSOME.key());

            query.remove(VariantQueryParam.GENE.key());

            String panelCypherQuery = getProteinNetworkCypher(query, options, includeAttrs, complexOrReaction);

            query.remove(VariantQueryParam.PANEL.key());
            query.put(VariantQueryParam.GENE.key(), geneValues);
            query.put(VariantQueryParam.ANNOT_BIOTYPE.key(), biotypeValues);
            query.put(VariantQueryParam.CHROMOSOME.key(), chromValues);

            String geneCypherQuery = getProteinNetworkCypher(query, options, includeAttrs, complexOrReaction);

            cypher = panelCypherQuery + "\nUNION\n" + geneCypherQuery;
        } else {
            cypher = getProteinNetworkCypher(query, options, includeAttrs, complexOrReaction);
        }

        System.out.println(cypher);
        return cypher;
    }

    //---------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //---------------------------------------------------------------

    private static List<String> getIncludeAttributes(Set<VariantQueryParam> includeMap, Set<VariantQueryParam> excludeMap) {
        Map<VariantQueryParam, String> attrMap = new HashMap<>();
        attrMap.put(VariantQueryParam.INCLUDE_STUDY, "attr_studies");
        attrMap.put(VariantQueryParam.INCLUDE_CONSEQUENCE_TYPE, "attr_consequenceTypes");
        attrMap.put(VariantQueryParam.INCLUDE_XREF, "attr_xrefs");
        attrMap.put(VariantQueryParam.INCLUDE_POPULATION_FREQUENCY, "attr_populationFrequencies");
        attrMap.put(VariantQueryParam.INCLUDE_CONSERVATION, "attr_conservation");
        attrMap.put(VariantQueryParam.INCLUDE_GENE_EXPRESSION, "attr_geneExpression");
        attrMap.put(VariantQueryParam.INCLUDE_GENE_TRAIT_ASSOCIATION, "attr_geneTraitAssociation");
        attrMap.put(VariantQueryParam.INCLUDE_GENE_DRUG_INTERACTION, "attr_geneDrugInteraction");
        attrMap.put(VariantQueryParam.INCLUDE_VARIANT_TRAIT_ASSOCIATION, "attr_variantTraitAssociation");
        attrMap.put(VariantQueryParam.INCLUDE_TRAIT_ASSOCIATION, "attr_traitAssociation");
        attrMap.put(VariantQueryParam.INCLUDE_FUNCTIONAL_SCORE, "attr_functionalScore");

        if (CollectionUtils.isEmpty(includeMap) && CollectionUtils.isEmpty(excludeMap)) {
            // No include, no exclude -> include ALL
            return new ArrayList<>(attrMap.values());
        } else if (CollectionUtils.isNotEmpty(includeMap)) {
            // Include
            Set<String> attrs = new HashSet<>();
            Iterator<VariantQueryParam> iterator = includeMap.iterator();
            while (iterator.hasNext()) {
                VariantQueryParam next = iterator.next();
                if (attrMap.containsKey(next)) {
                    attrs.add(attrMap.get(next));
                }
            }
            return new ArrayList<>(attrs);
        } else {
            // Exclude
            Iterator<VariantQueryParam> iterator = excludeMap.iterator();
            while (iterator.hasNext()) {
                VariantQueryParam next = iterator.next();
                if (next == VariantQueryParam.EXCLUDE_STUDY) {
                    attrMap.remove(VariantQueryParam.INCLUDE_STUDY);
                }
                if (next == VariantQueryParam.EXCLUDE_CONSEQUENCE_TYPE) {
                    attrMap.remove(VariantQueryParam.INCLUDE_CONSEQUENCE_TYPE);
                }
                if (next == VariantQueryParam.EXCLUDE_XREF) {
                    attrMap.remove(VariantQueryParam.INCLUDE_XREF);
                }
                if (next == VariantQueryParam.EXCLUDE_POPULATION_FREQUENCY) {
                    attrMap.remove(VariantQueryParam.INCLUDE_POPULATION_FREQUENCY);
                }
                if (next == VariantQueryParam.EXCLUDE_CONSERVATION) {
                    attrMap.remove(VariantQueryParam.INCLUDE_CONSERVATION);
                }
                if (next == VariantQueryParam.EXCLUDE_GENE_EXPRESSION) {
                    attrMap.remove(VariantQueryParam.INCLUDE_GENE_EXPRESSION);
                }
                if (next == VariantQueryParam.EXCLUDE_GENE_TRAIT_ASSOCIATION) {
                    attrMap.remove(VariantQueryParam.INCLUDE_GENE_TRAIT_ASSOCIATION);
                }
                if (next == VariantQueryParam.EXCLUDE_GENE_DRUG_INTERACTION) {
                    attrMap.remove(VariantQueryParam.INCLUDE_GENE_DRUG_INTERACTION);
                }
                if (next == VariantQueryParam.EXCLUDE_VARIANT_TRAIT_ASSOCIATION) {
                    attrMap.remove(VariantQueryParam.INCLUDE_VARIANT_TRAIT_ASSOCIATION);
                }
                if (next == VariantQueryParam.EXCLUDE_TRAIT_ASSOCIATION) {
                    attrMap.remove(VariantQueryParam.INCLUDE_TRAIT_ASSOCIATION);
                }
                if (next == VariantQueryParam.EXCLUDE_FUNCTIONAL_SCORE) {
                    attrMap.remove(VariantQueryParam.INCLUDE_FUNCTIONAL_SCORE);
                }
            }
            return new ArrayList<>(attrMap.values());
        }
    }

    private static Set<VariantQueryParam> getIncludeMap(Query query) {
        Set<VariantQueryParam> includes = new HashSet<>();

        if (query.containsKey(VariantQueryParam.INCLUDE_STUDY.key())) {
            includes.add(VariantQueryParam.INCLUDE_STUDY);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_CONSEQUENCE_TYPE.key())) {
            includes.add(VariantQueryParam.INCLUDE_CONSEQUENCE_TYPE);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_XREF.key())) {
            includes.add(VariantQueryParam.INCLUDE_XREF);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_POPULATION_FREQUENCY.key())) {
            includes.add(VariantQueryParam.INCLUDE_POPULATION_FREQUENCY);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_CONSERVATION.key())) {
            includes.add(VariantQueryParam.INCLUDE_CONSERVATION);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_GENE_EXPRESSION.key())) {
            includes.add(VariantQueryParam.INCLUDE_GENE_EXPRESSION);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_GENE_TRAIT_ASSOCIATION.key())) {
            includes.add(VariantQueryParam.INCLUDE_GENE_TRAIT_ASSOCIATION);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_GENE_DRUG_INTERACTION.key())) {
            includes.add(VariantQueryParam.INCLUDE_GENE_DRUG_INTERACTION);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_VARIANT_TRAIT_ASSOCIATION.key())) {
            includes.add(VariantQueryParam.INCLUDE_VARIANT_TRAIT_ASSOCIATION);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_TRAIT_ASSOCIATION.key())) {
            includes.add(VariantQueryParam.INCLUDE_TRAIT_ASSOCIATION);
        }
        if (query.containsKey(VariantQueryParam.INCLUDE_FUNCTIONAL_SCORE.key())) {
            includes.add(VariantQueryParam.INCLUDE_FUNCTIONAL_SCORE);
        }
        return includes;
    }

    private static Set<VariantQueryParam> getExcludeMap(Query query) {
        Set<VariantQueryParam> excludes = new HashSet<>();

        if (query.containsKey(VariantQueryParam.EXCLUDE_STUDY.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_STUDY);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_CONSEQUENCE_TYPE.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_CONSEQUENCE_TYPE);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_XREF.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_XREF);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_POPULATION_FREQUENCY.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_POPULATION_FREQUENCY);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_CONSERVATION.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_CONSERVATION);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_GENE_EXPRESSION.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_GENE_EXPRESSION);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_GENE_TRAIT_ASSOCIATION.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_GENE_TRAIT_ASSOCIATION);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_GENE_DRUG_INTERACTION.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_GENE_DRUG_INTERACTION);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_VARIANT_TRAIT_ASSOCIATION.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_VARIANT_TRAIT_ASSOCIATION);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_TRAIT_ASSOCIATION.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_TRAIT_ASSOCIATION);
        }
        if (query.containsKey(VariantQueryParam.EXCLUDE_FUNCTIONAL_SCORE.key())) {
            excludes.add(VariantQueryParam.EXCLUDE_FUNCTIONAL_SCORE);
        }
        return excludes;
    }

    public static List<Neo4JQueryParser.CypherStatement> getCypherStatements(Query query, QueryOptions queryOptions) {
        List<Neo4JQueryParser.CypherStatement> cypherStatements = new ArrayList<>();

        // Chromosome
        String chromWhere = "";
        String param = VariantQueryParam.CHROMOSOME.key();
        if (query.containsKey(param)) {
            List<String> chromosomes = Arrays.asList(query.getString(param).split(","));
            chromWhere = getConditionString(chromosomes, "v.attr_chromosome", true);
        }

        // Panel
        if (query.containsKey(VariantQueryParam.PANEL.key())) {
            cypherStatements.addAll(parsePanel(query.getString(VariantQueryParam.PANEL.key()),
                    query.getString(VariantQueryParam.ANNOT_BIOTYPE.key()), chromWhere));
            chromWhere = "";
        }

        // Gene
        if (query.containsKey(VariantQueryParam.GENE.key())) {
            cypherStatements.addAll(parseGene(query.getString(VariantQueryParam.GENE.key()),
                    query.getString(VariantQueryParam.ANNOT_BIOTYPE.key()), chromWhere));
            chromWhere = "";
        }

        // SO
        if (query.containsKey(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())) {
            String biotypeValues = "";
            if (!query.containsKey(VariantQueryParam.PANEL.key()) && query.containsKey(VariantQueryParam.ANNOT_BIOTYPE.key())) {
                biotypeValues = query.getString(VariantQueryParam.ANNOT_BIOTYPE.key());
            }
            cypherStatements.add(parseConsequenceType(query.getString(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()), biotypeValues,
                    chromWhere));
            chromWhere = "";
        }

        // Genotype (sample)
        // chromWhere should be used only once, not every genotype iteration
        if (query.containsKey(VariantQueryParam.GENOTYPE.key())) {
            cypherStatements.addAll(parseGenotype(query.getString(VariantQueryParam.GENOTYPE.key()), chromWhere));
            chromWhere = "";
        }

        // Biotype
        param = VariantQueryParam.ANNOT_BIOTYPE.key();
        if (query.containsKey(param) && !query.containsKey(VariantQueryParam.PANEL.key())
                && !query.containsKey(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())) {
            cypherStatements.add(parseBiotype(param, chromWhere));
            chromWhere = "";
        }

        // Population frequency (alternate frequency)
        param = VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key();
        if (query.containsKey(param)) {
            cypherStatements.addAll(parsePopulationFrequency(query.getString(param), chromWhere));
        }

        return cypherStatements;
    }

    private static String buildCypherStatement(Query query, List<String> includeAttrs,
                                               List<Neo4JQueryParser.CypherStatement> cypherStatements) {

        int i = 0;
        Neo4JQueryParser.CypherStatement st;
        StringBuilder sb = new StringBuilder();
        for (i = 0; i < cypherStatements.size() - 1; i++) {
            st = cypherStatements.get(i);
            sb.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append(st.getWith()).append("\n");
        }
        st = cypherStatements.get(i);
        sb.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append(st.getWith()).append("\n");

        if (query.containsKey(VariantQueryParam.GENOTYPE.key()) && query.getBoolean(VariantQueryParam.INCLUDE_GENOTYPE.key())) {
            sb.append("MATCH (s:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(v:VARIANT)\n")
                    .append("WITH DISTINCT v, collect(s.id) AS ").append(NodeBuilder.SAMPLE)
                    .append(", collect(vc.attr_GT) AS ").append(NodeBuilder.GENOTYPE)
                    .append("\n");
        }

        sb.append("MATCH (v)-[:VARIANT__VARIANT_OBJECT]-(vo:VARIANT_OBJECT) ");
        sb.append("RETURN DISTINCT vo.attr_core AS attr_core");
        for (String includeAttr : includeAttrs) {
            sb.append(", ").append("vo.").append(includeAttr).append(" AS ").append(includeAttr);
        }

        if (query.containsKey(VariantQueryParam.GENOTYPE.key()) && query.getBoolean(VariantQueryParam.INCLUDE_GENOTYPE.key())) {
            sb.append(", ").append(NodeBuilder.SAMPLE).append(", ").append(NodeBuilder.GENOTYPE);
        }
        return sb.toString();
    }

    public static List<Neo4JQueryParser.CypherStatement> parsePanel(String panelValues, String biotypeValues, String chromWhere) {
        List<String> panels = Arrays.asList(panelValues.split(","));
        List<Neo4JQueryParser.CypherStatement> cypherStatements = new ArrayList<>();


        // Match1
        String match = "MATCH (p:PANEL)-[:PANEL__GENE]-(:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)";

        // Where1
        String where = "WHERE " + getConditionString(panels, "p.name", false);

        // With1
        String with = "WITH DISTINCT tr1";

        cypherStatements.add(new Neo4JQueryParser.CypherStatement(match, where, with));

        cypherStatements.add(getTranscriptMatch(biotypeValues, chromWhere));

        return cypherStatements;
    }

    public static List<Neo4JQueryParser.CypherStatement> parseGene(String geneValues, String biotypeValues, String chromWhere) {
        List<String> genes = Arrays.asList(geneValues.split(","));
        List<Neo4JQueryParser.CypherStatement> cypherStatements = new ArrayList<>();

        // Match1
        String match = "MATCH (r:XREF)-[:XREF]-(:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)";

        // Where1
        String where = "WHERE " + getConditionString(genes, "r.id", false);

        // With1
        String with = "WITH DISTINCT tr1";

        cypherStatements.add(new Neo4JQueryParser.CypherStatement(match, where, with));

        cypherStatements.add(getTranscriptMatch(biotypeValues, chromWhere));

        return cypherStatements;
    }

    private static Neo4JQueryParser.CypherStatement getTranscriptMatch(String biotypeValues, String chromWhere) {
        // Match2
        String match = "MATCH (tr1:TRANSCRIPT)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(ct:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]"
                + "-(v:VARIANT)";

        // Where2
        String where = "";
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(biotypeValues)) {
            where = "WHERE " + (getConditionString(Arrays.asList(biotypeValues.split(",")), "ct.attr_biotype", false)) + chromWhere;
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(chromWhere)) {
            where = chromWhere.replace("AND", "WHERE");
        }

        // With2
        String with = "WITH DISTINCT v";

        return new Neo4JQueryParser.CypherStatement(match, where, with);
    }

    private static List<Neo4JQueryParser.CypherStatement> parseGenotype(String genotypeValues, String chromWhere) {
        List<Neo4JQueryParser.CypherStatement> cypherStatements = new ArrayList<>();

        HashMap<Object, List<String>> map = new LinkedHashMap<>();
        VariantQueryUtils.QueryOperation queryOperation = VariantQueryUtils.parseGenotypeFilter(genotypeValues, map);
        List<String> samples = new ArrayList<>(map.size());
        map.keySet().stream().map(Object::toString).forEach(samples::add);

        Iterator<Object> sampleIterator = map.keySet().iterator();
        while (sampleIterator.hasNext()) {
            String sample = sampleIterator.next().toString();
            if (map.get(sample).size() > 0) {
                // Match
                String match = "MATCH (s:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(v:VARIANT)";

                // Where
                String where = "WHERE " + getConditionString(Collections.singletonList(sample), "s.id", false)
                        + getConditionString(map.get(sample), "vc.attr_GT", true) + chromWhere;

                // With
                String with = "WITH DISTINCT v";

                cypherStatements.add(new Neo4JQueryParser.CypherStatement(match, where, with));
            }
        }
        return cypherStatements;
    }

    private static Neo4JQueryParser.CypherStatement parseConsequenceType(String ctValues, String biotypeValues, String chromWhere) {
        List<String> cts = Arrays.asList(ctValues.split(","));

        // Match
        String match = "MATCH (so:SO)-[:CONSEQUENCE_TYPE__SO]-(ct:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-(v:VARIANT)";

        // Where
        String where = "WHERE " + getConditionString(cts, "so.name", false) + chromWhere;
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(biotypeValues)) {
            where += (getConditionString(Arrays.asList(biotypeValues.split(",")), "ct.attr_biotype", true));
        }

        // With
        String with = "WITH DISTINCT v";

        return new Neo4JQueryParser.CypherStatement(match, where, with);
    }

    private static Neo4JQueryParser.CypherStatement parseBiotype(String biotypeValues, String chromWhere) {
        List<String> biotypes = Arrays.asList(biotypeValues.split(","));

        // Match
        String match = "MATCH (ct:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-(v:VARIANT)";

        // Where
        String where = "WHERE " + getConditionString(biotypes, "ct.attr_biotype", false) + chromWhere;

        // With
        String with = "WITH DISTINCT v";

        return new Neo4JQueryParser.CypherStatement(match, where, with);
    }

    private static List<Neo4JQueryParser.CypherStatement> parsePopulationFrequency(String popFreqValues, String chromWhere) {
        List<Neo4JQueryParser.CypherStatement> cypherStatements = new ArrayList<>();

        String[] popFreqs = popFreqValues.split("[,;]");

        Matcher matcher;
        if (popFreqValues.contains(",")) {
            // OR -> we need only one MATCH statement for all pop. frequencies

            // Math
            String match = "MATCH (v:VARIANT)-[:VARIANT__POPULATION_FREQUENCY]-(pf:POPULATION_FREQUENCY)";

            // Where
            boolean first = true;
            StringBuilder where = new StringBuilder("WHERE ");
            for (String popFreq : popFreqs) {
                matcher = POP_FREQ_PATTERN.matcher(popFreq);
                if (matcher.find()) {
                    if (!first) {
                        where.append(" OR ");
                    }
                    where.append("(pf.id = '" + matcher.group(1)).append("' AND toFloat(pf.attr_altAlleleFreq)")
                            .append(matcher.group(2)).append(matcher.group(3)).append(")");
                    first = false;
                } else {
                    throw new InvalidParameterException("Invalid population frequency parameter: " + popFreq);
                }
            }
            where.append(chromWhere);

            // With
            String with = "WITH DISTINCT v";

            cypherStatements.add(new Neo4JQueryParser.CypherStatement(match, where.toString(), with));
        } else {
            // AND -> we need one MATCH per population frequency

            for (String popFreq : popFreqs) {
                matcher = POP_FREQ_PATTERN.matcher(popFreq);
                if (matcher.find()) {
                    // Match
                    String match = "MATCH (v:VARIANT)-[:VARIANT__POPULATION_FREQUENCY]-(pf:POPULATION_FREQUENCY)";

                    // Where
                    String where = "WHERE (pf.id = '" + matcher.group(1) + "' AND toFloat(pf.attr_altAlleleFreq)" + matcher.group(2)
                            + matcher.group(3) + ")" + chromWhere;
                    chromWhere = "";

                    // With
                    String with = "WITH DISTINCT v";

                    cypherStatements.add(new Neo4JQueryParser.CypherStatement(match, where, with));
                } else {
                    throw new InvalidParameterException("Invalid population frequency parameter: " + popFreq);
                }
            }
        }
        return cypherStatements;
    }

    /**
     * Builds the part of the cypher query aimed to act as a searching filter. We can fiter by the individual samples, their
     * genotype, the chromosome or the genes in which we want to look up.
     * <p>
     * [Mainly used for methods "getVariantsFromPedigree" and "getVariantsFromList"]
     *
     * @param stringList The list of elements that will compound the filter
     * @param calling    The index we want to use to call if from the database
     * @param isNotFirst A boolean that adds an "AND" operator at the start of the substring if needed
     * @return the substring with the filter ready to use for Neo4j
     */
    public static String getConditionString(List<String> stringList, String calling, boolean isNotFirst) {
        return getConditionString(stringList, calling, "=", " OR ", isNotFirst);
    }

    private static String getConditionString(List<String> stringList, String calling, String op, String logicalOp, boolean isNotFirst) {
        String substring = "";
        if (stringList.size() == 0) {
            return substring;
        } else {
            List<String> elements = new ArrayList<>();
            for (String element : stringList) {
                elements.add(substring + calling + op + "'" + element + "'");
            }
            substring = StringUtils.join(elements, logicalOp);
            substring = "(" + substring + ")";
            if (isNotFirst) {
                substring = " AND " + substring;
            }
            return substring;
        }
    }

    private static String getProteinNetworkCypher(Query query, QueryOptions options, List<String> includeAttrs, boolean complexOrReaction) {
        StringBuilder cypher = new StringBuilder();

        if (!query.containsKey(VariantQueryParam.PANEL.key()) && !query.containsKey(VariantQueryParam.GENE.key())) {
            throw new IllegalArgumentException("Missing panels and gene list. At leat one of them must be specified.");
        }

        // Chromosome
        String chromWhere = "";
        String param = VariantQueryParam.CHROMOSOME.key();
        if (query.containsKey(param)) {
            List<String> chromosomes = Arrays.asList(query.getString(param).split(","));
            chromWhere = getConditionString(chromosomes, "v.attr_chromosome", true);
        }

        String nexus = complexOrReaction ? COMPLEX : REACTION;

        // Match1
        cypher.append("MATCH (tr1:TRANSCRIPT)-[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-");
        if (complexOrReaction) {
            cypher.append("[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-");
        } else {
            cypher.append("[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-");
        }

        // PanelTail
        if (query.containsKey(VariantQueryParam.PANEL.key())) {
            cypher.append(parsePanelTail(query));
        }

        // GeneTail
        if (query.containsKey(VariantQueryParam.GENE.key())) {
            cypher.append(parseGeneTail(query));
        }

        // With1
        cypher.append("WITH DISTINCT tr1, prot1.name AS ").append(TARGET_PROTEIN).append(", nex.name AS ").append(nexus)
                .append(", prot2.name AS ").append(PANEL_PROTEIN).append(", g.id AS ").append(PANEL_GENE).append("\n");

        // Match2
        cypher.append("MATCH (v:VARIANT)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__TRANSCRIPT]")
                .append("-(tr1:TRANSCRIPT)").append("\n");

        // Where2
        String biotypeValues = query.getString(VariantQueryParam.ANNOT_BIOTYPE.key());
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(biotypeValues)) {
            cypher.append("WHERE ").append(getConditionString(Arrays.asList(biotypeValues.split(",")), "ct.attr_biotype", false))
                    .append(chromWhere).append("\n");
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(chromWhere)) {
            cypher.append(chromWhere.replace("AND", "WHERE")).append("\n");
        }

        // With2
        String systemParams = ", " + TARGET_PROTEIN + ", " + nexus + ", " + PANEL_PROTEIN + ", " + PANEL_GENE;
        cypher.append("WITH DISTINCT v").append(systemParams).append("\n");

        query.remove(VariantQueryParam.PANEL.key());
        query.remove(VariantQueryParam.GENE.key());
        query.remove(VariantQueryParam.ANNOT_BIOTYPE.key());
        query.remove(VariantQueryParam.CHROMOSOME.key());
        List<Neo4JQueryParser.CypherStatement> cypherStatements = getCypherStatements(query, options);

        int i;
        Neo4JQueryParser.CypherStatement st;
        for (i = 0; i < cypherStatements.size() - 1; i++) {
            st = cypherStatements.get(i);
            cypher.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append(st.getWith()).append(systemParams)
                    .append("\n");
        }
        st = cypherStatements.get(i);
        cypher.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append("WITH DISTINCT v").append(systemParams)
                .append("\n").append("MATCH (s:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(v:VARIANT)")
                .append("\n").append("WITH DISTINCT v, collect(s.id) AS ").append(NodeBuilder.SAMPLE)
                .append(", collect(vc.attr_GT) AS ").append(NodeBuilder.GENOTYPE)
                .append("\n");

        cypher.append("MATCH (v)-[:VARIANT__VARIANT_OBJECT]-(vo:VARIANT_OBJECT) ");
        cypher.append("RETURN DISTINCT vo.attr_core AS attr_core");
        for (String includeAttr : includeAttrs) {
            cypher.append(", ").append("vo.").append(includeAttr).append(" AS ").append(includeAttr);
        }
        cypher.append(", ").append(NodeBuilder.SAMPLE).append(", ").append(NodeBuilder.GENOTYPE);

        return cypher.toString();
    }

    private static StringBuilder parsePanelTail(Query query) {
        StringBuilder panelTail = new StringBuilder();

        // Match1 tail
        panelTail.append("(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(g:GENE)-[:PANEL__GENE]-(p:PANEL)\n");

        // Where1
        panelTail.append("WHERE ").append(getConditionString(Arrays.asList(query.getString(VariantQueryParam.PANEL.key())
                .split(",")), "p.name", false)).append("\n");
        return panelTail;
    }

    private static StringBuilder parseGeneTail(Query query) {
        StringBuilder panelTail = new StringBuilder();

        // Match1 tail
        panelTail.append("(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(g:GENE)-[:XREF]-(r:XREF)\n");

        // Where1
        panelTail.append("WHERE ").append(getConditionString(Arrays.asList(query.getString(VariantQueryParam.GENE.key())
                .split(",")), "r.id", false)).append("\n");
        return panelTail;
    }
}
