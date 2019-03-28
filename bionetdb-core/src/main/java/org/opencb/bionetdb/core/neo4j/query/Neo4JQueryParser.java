package org.opencb.bionetdb.core.neo4j.query;

import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.query.NetworkPathQuery;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by imedina on 03/09/15.
 */
public class Neo4JQueryParser {

    //public static final Pattern operationPattern = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

    private static final Set<String> GENE_PSEUDO_ATTRS = new HashSet<>(Arrays.asList("drug", "hpo", "go"));
    private static final Set<String> VARIANT_PSEUDO_ATTRS = new HashSet<>(Arrays.asList("popFreq", "so"));

    private static final Pattern POP_FREQ_PATTERN = Pattern.compile("([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");

    public static class CypherStatement {
        private String match;
        private String where;
        private String with;

        public CypherStatement() {
        }

        public CypherStatement(String match, String where, String with) {
            this.match = match;
            this.where = where;
            this.with = with;
        }

        public String getMatch() {
            return match;
        }

        public void setMatch(String match) {
            this.match = match;
        }

        public String getWhere() {
            return where;
        }

        public void setWhere(String where) {
            this.where = where;
        }

        public String getWith() {
            return with;
        }

        public void setWith(String with) {
            this.with = with;
        }
    }

    public static String parseNodeQuery(Query query, QueryOptions options) throws BioNetDBException {
        String nameNode = "n";

        // Match clause
        StringBuilder match = new StringBuilder();
        match.append("MATCH (").append(nameNode);
//        if (query.getType() != null) {
//            match.append(":").append(query.getType().name());
//        }
        match.append(")");

        // Where clauses, parse attributes and relationships
        StringBuilder where = new StringBuilder();
        List<String> filters = getAttributeFilters(nameNode, query, options);
        if (filters.size() > 0) {
            where.append(" WHERE ").append(StringUtils.join(filters, " AND "));
        }

//        // Parse pseudo-attributes
//        if (query.getType() == Node.Type.GENE) {
//            where.append(parseGeneNode(query, options));
//        } else if (query.getType() == Node.Type.VARIANT) {
//            where.append(parseVariantNode(query, options));
//        }

        // Return clauses
        StringBuilder ret = new StringBuilder();
        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> includes = options.getAsStringList(QueryOptions.INCLUDE);
            ret.append(" RETURN ").append(nameNode).append(".").append(includes.get(0));
            for (int i = 1; i < includes.size(); i++) {
                ret.append(", ").append(nameNode).append(".").append(includes.get(i));
            }
        } else {
            ret.append(" RETURN ").append(nameNode);
        }

        // Build the complete Cypher statement
        StringBuilder cypher = new StringBuilder();
        cypher.append(match).append(where).append(ret);

        return cypher.toString();
    }

    public static String parsePath(NetworkPathQuery query, QueryOptions options) throws BioNetDBException {
        if (query.getSrcNodeQuery() == null || query.getDestNodeQuery() == null) {
            throw new BioNetDBException("Invalid path query: it is madatory to include source and destination nodes");
        }

        StringBuilder match = new StringBuilder();
        String srcNodeName = "n1";
        String destNodeName = "n2";
        NodeQuery srcQuery = query.getSrcNodeQuery();
        NodeQuery destQuery = query.getDestNodeQuery();

        // Max jumps
        int maxJumps = query.getInt(NetworkDBAdaptor.NetworkQueryParams.MAX_JUMPS.key(), 2);

        // Match clause
        match.append("MATCH path=(").append(srcNodeName);
        if (srcQuery.getType() != null) {
            match.append(":").append(srcQuery.getType().name());
        }
        match.append(")-[*..").append(maxJumps).append("]-(").append(destNodeName);
        if (destQuery.getType() != null) {
            match.append(":").append(destQuery.getType().name());
        }
        match.append(")");

        // Where clauses, parse attributes
        StringBuilder where = new StringBuilder();
        List<String> filters = getAttributeFilters(srcNodeName, query.getSrcNodeQuery(), options);
        filters.addAll(getAttributeFilters(destNodeName, query.getDestNodeQuery(), options));
        if (filters.size() > 0) {
            where.append(" WHERE ").append(StringUtils.join(filters, " AND "));
        }

        // Build the complete Cypher statement
        StringBuilder cypher = new StringBuilder();
        cypher.append(match).append(where).append(" RETURN path");

        return cypher.toString();
    }

    public static String parseNodesForNetwork(List<NodeQuery> nodeQueries, QueryOptions options) throws BioNetDBException {
        List<NetworkPathQuery> pathQueries = new ArrayList<>();
        int size = nodeQueries.size();
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                NetworkPathQuery networkPathQuery = new NetworkPathQuery(nodeQueries.get(i), nodeQueries.get(j));
                pathQueries.add(networkPathQuery);
            }
        }

        return parsePathsForNetwork(pathQueries, options);
    }

    public static String parsePathsForNetwork(List<NetworkPathQuery> pathQueries, QueryOptions options) throws BioNetDBException {
        StringBuilder cypher = new StringBuilder();
        if (ListUtils.isNotEmpty(pathQueries)) {
            cypher.append(parsePath(pathQueries.get(0), options));
            for (int i = 1; i < pathQueries.size(); i++) {
                cypher.append(" UNION ").append(parsePath(pathQueries.get(i), options));
            }
        }
        return cypher.toString();
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------


    private static List<String> getAttributeFilters(String nodeName, Query query, QueryOptions options) {
        // Get pseudo-attributes for the target node
        Set<String> skip;
//        if (query.getType() == Node.Type.GENE) {
//            skip = GENE_PSEUDO_ATTRS;
//        } else if (query.getType() == Node.Type.VARIANT) {
//            skip = VARIANT_PSEUDO_ATTRS;
//        } else {
        skip = new HashSet<>();
//        }

        // Only process the non pseudo-attributes
        List<String> filters = new ArrayList<>();
        for (String key : query.keySet()) {
            if (skip.contains(key)) {
                continue;
            }
            StringBuilder filter = new StringBuilder(nodeName).append(".").append(key);
            String value = query.getString(key);
            if (value.startsWith("=") || value.startsWith(">") || value.startsWith("<") || value.startsWith("!")) {
                filter.append(query.get(key));
            } else if (query.get(key) instanceof String) {
                filter.append("=\"").append(query.get(key)).append("\"");
            } else {
                filter.append("=").append(query.get(key));
            }
            filters.add(filter.toString());
        }
        return filters;
    }

    private static String parseGeneNode(NodeQuery query, QueryOptions options) {
        StringBuilder cypher = new StringBuilder();
        return cypher.toString();
    }

    private static String parseVariantNode(NodeQuery query, QueryOptions options) {
        StringBuilder cypher = new StringBuilder();
        return cypher.toString();
    }

    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------

    public static String parse(Query query, QueryOptions options) throws BioNetDBException {
        int counter = 1;

        StringBuilder match = new StringBuilder();
        String srcNode = null, destNode = null;

        // Source node
        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key()))) {
            srcNode = query.getString(NetworkDBAdaptor.NetworkQueryParams.SRC_NODE.key());
        }

        // Destination node
        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key()))) {
            destNode = query.getString(NetworkDBAdaptor.NetworkQueryParams.DEST_NODE.key());
        }

        // Max jumps
        int maxJumps = query.getInt(NetworkDBAdaptor.NetworkQueryParams.MAX_JUMPS.key(), 2);

        // Output
        String output = "network";
        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key()))) {
            output = query.getString(NetworkDBAdaptor.NetworkQueryParams.OUTPUT.key());
        }

        if (StringUtils.isEmpty(srcNode) || StringUtils.isEmpty(destNode)) {
            String inputNode = (StringUtils.isNotEmpty(srcNode) ? srcNode : destNode);
            if (StringUtils.isNotEmpty(inputNode)) {
                Map<String, String> aliasMap = new HashMap<>();
                List<String> where = new ArrayList<>();

                // Source node
                String[] split = inputNode.split(":");
                String alias = "n" + (counter++);
                String inputLabel = alias + ":" + split[0];
                aliasMap.put(split[0], alias);
                if (split.length > 1) {
                    where.add(alias + "." + split[1]);
                }
                match.append("MATCH path=(").append(inputLabel).append(")");
                if (ListUtils.isNotEmpty(where)) {
                    match.append(" WHERE ").append(StringUtils.join(where, " AND "));
                }
                match.append(getReturnStatment(output, aliasMap));
            }
        } else {
            Map<String, String> aliasMap = new HashMap<>();
            List<String> where = new ArrayList<>();

            // Source node
            String[] split = srcNode.split(":");
            String alias = "n" + (counter++);
            String srcLabel = alias + ":" + split[0];
            aliasMap.put(split[0], alias);
            if (split.length > 1) {
                where.add(alias + "." + split[1]);
            }

            // Dest node
            split = destNode.split(":");
            alias = "n" + (counter++);
            String destLabel = alias + ":" + split[0];
            aliasMap.put(split[0], alias);
            if (split.length > 1) {
                where.add(alias + "." + split[1]);
            }

            match.append("MATCH path=(").append(srcLabel).append(")-[*..").append(maxJumps).append("]-(").append(destLabel).append(")");
            if (ListUtils.isNotEmpty(where)) {
                match.append(" WHERE ").append(StringUtils.join(where, " AND "));
            }
            match.append(getReturnStatment(output, aliasMap));
        }
        return match.toString();
    }

    private static String getReturnStatment(String output, Map<String, String> aliasMap) {
        StringBuilder retStatement = new StringBuilder();
        retStatement.append(" RETURN ");

        String[] split;
        if ("network".equals(output)) {
            // Output as network
            retStatement.append("path");
        } else if (output.contains(".")) {
            // Output as table
            List<String> ret = new ArrayList<>();
            String[] nodes = output.split(",");
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].contains(".")) {
                    split = nodes[i].split("\\.");
                    if (StringUtils.isNotEmpty(aliasMap.get(split[0]))) {
                        ret.add(aliasMap.get(split[0]) + "." + split[1]);
                    }
                }
            }
            retStatement.append(StringUtils.join(ret, ", "));
        } else {
            // Output as node
            List<String> ret = new ArrayList<>();
            String[] nodes = output.split(",");
            for (int i = 0; i < nodes.length; i++) {
                if (StringUtils.isNotEmpty(aliasMap.get(nodes[i]))) {
                    ret.add(aliasMap.get(nodes[i]));
                }
            }
            retStatement.append(StringUtils.join(ret, ","));
        }

        return retStatement.toString();
    }

    public static String parse(String nodeName, Query query, QueryOptions options) throws BioNetDBException {
        if (query == null) {
            query = new Query();
        }

        List<String> myMatchList = new ArrayList<>();
        List<String> myWhereClauses = new ArrayList<>();
        StringBuilder cypherQuery = new StringBuilder();

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key()))) {
            myMatchList.add("(" + nodeName + ":" + query.getString(NetworkDBAdaptor.NetworkQueryParams.NODE_TYPE.key()
                    .replace(',', '|')).toUpperCase() + ")");
        }
//        else {
//            myMatchList.add("(" + nodeName + ")");
//        }

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.NODE_UID.key()))) {
            myWhereClauses.add(nodeName + ".uid=" + query.getString(NetworkDBAdaptor.NetworkQueryParams.NODE_UID.key()));
        }

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.NODE_ID.key()))) {
            myWhereClauses.add(nodeName + ".id=\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.NODE_ID.key())
                    + "\"");
        }

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()))) {
            myMatchList.add("(" + nodeName + ")-[:XREF]->(" + nodeName + "x:XREF)");
            myWhereClauses.add(nodeName + "x.id IN [\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key())
                    .replace(",", "\", \"") + "\"]");
        }

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()))) {
            myMatchList.add("(" + nodeName + ")-[:ONTOLOGY]->(" + nodeName + "o:ONTOLOGY)");
            myWhereClauses.add(nodeName + "o.id IN [\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key())
                    .replace(",", "\", \"") + "\"]");
        }

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key()))) {
            myMatchList.add("(" + nodeName + ")-[:CELLULAR_LOCATION]->(" + nodeName + "c:CELLULAR_LOCATION)");
            myWhereClauses.add(nodeName + "c.name IN [\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key())
                    .replace(",", "\", \"") + "\"]");
        }

        cypherQuery.append(StringUtils.join(myMatchList, ','));

        if (myWhereClauses.size() > 0) {
            cypherQuery.append(" WHERE ");
            cypherQuery.append(StringUtils.join(myWhereClauses, " AND "));
        }
        return cypherQuery.toString();
    }

    //---------------------------------------------------------------------
    // M O I - P R O T E I N   S Y S T E M   M E T H O D S
    //---------------------------------------------------------------------

    public static String parseVariantQuery(Query query, QueryOptions options) {
        String cypher;

        if (query.containsKey(Neo4JVariantQueryParam.PANEL.key()) && query.containsKey(Neo4JVariantQueryParam.GENE.key())) {
            String geneValues = query.getString(Neo4JVariantQueryParam.GENE.key());
            query.remove(Neo4JVariantQueryParam.GENE.key());

            List<CypherStatement> panelCypherStatements = getCypherStatements(query, options);

            query.remove(Neo4JVariantQueryParam.PANEL.key());
            query.put(Neo4JVariantQueryParam.GENE.key(), geneValues);

            List<CypherStatement> geneCypherStatements = getCypherStatements(query, options);

            cypher = buildCypherStatement(query, panelCypherStatements) + "\nUNION\n" + buildCypherStatement(query, geneCypherStatements);
        } else {
            cypher = buildCypherStatement(query, getCypherStatements(query, options));
        }
//        System.out.println(cypher);
        return cypher;
    }

    public static List<CypherStatement> getCypherStatements(Query query, QueryOptions queryOptions) {
        List<CypherStatement> cypherStatements = new ArrayList<>();

        // Chromosome
        String chromWhere = "";
        String param = Neo4JVariantQueryParam.CHROMOSOME.key();
        if (query.containsKey(param)) {
            List<String> chromosomes = Arrays.asList(query.getString(param).split(","));
            chromWhere = getConditionString(chromosomes, "v.attr_chromosome", true);
        }

        // Panel
        if (query.containsKey(Neo4JVariantQueryParam.PANEL.key())) {
            cypherStatements.addAll(parsePanel(query.getString(Neo4JVariantQueryParam.PANEL.key()),
                    query.getString(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key()), chromWhere));
            chromWhere = "";
        }

        // Gene
        if (query.containsKey(Neo4JVariantQueryParam.GENE.key())) {
            cypherStatements.addAll(parseGene(query.getString(Neo4JVariantQueryParam.GENE.key()),
                    query.getString(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key()), chromWhere));
            chromWhere = "";
        }

        // SO
        if (query.containsKey(Neo4JVariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())) {
            String biotypeValues = "";
            if (!query.containsKey(Neo4JVariantQueryParam.PANEL.key()) && query.containsKey(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key())) {
                biotypeValues = query.getString(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key());
            }
            cypherStatements.add(parseConsequenceType(query.getString(Neo4JVariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()), biotypeValues,
                    chromWhere));
            chromWhere = "";
        }

        // Genotype (sample)
        // chromWhere should be used only once, not every genotype iteration
        if (query.containsKey(Neo4JVariantQueryParam.GENOTYPE.key())) {
            cypherStatements.addAll(parseGenotype(query.getString(Neo4JVariantQueryParam.GENOTYPE.key()), chromWhere));
            chromWhere = "";
        }

        // Biotype
        param = Neo4JVariantQueryParam.ANNOT_BIOTYPE.key();
        if (query.containsKey(param) && !query.containsKey(Neo4JVariantQueryParam.PANEL.key())
                && !query.containsKey(Neo4JVariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())) {
            cypherStatements.add(parseBiotype(param, chromWhere));
            chromWhere = "";
        }


        // Population frequency (alternate frequency)
        param = Neo4JVariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key();
        if (query.containsKey(param)) {
            cypherStatements.addAll(parsePopulationFrequency(query.getString(param), chromWhere));
        }

        return cypherStatements;
    }

    private static String buildCypherStatement(Query query, List<CypherStatement> cypherStatements) {
        int i = 0;
        CypherStatement st;
        StringBuilder sb = new StringBuilder();
        for (i = 0; i < cypherStatements.size() - 1; i++) {
            st = cypherStatements.get(i);
            sb.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append(st.getWith()).append("\n");
        }
        st = cypherStatements.get(i);
        sb.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n");
        if (!query.getBoolean(Neo4JVariantQueryParam.INCLUDE_GENOTYPE.key())
                && !query.getBoolean(Neo4JVariantQueryParam.INCLUDE_CONSEQUENCE_TYPE.key())) {
            sb.append("RETURN DISTINCT v");
        } else if (query.getBoolean(Neo4JVariantQueryParam.INCLUDE_GENOTYPE.key())
                && !query.getBoolean(Neo4JVariantQueryParam.INCLUDE_CONSEQUENCE_TYPE.key())) {
            sb.append("WITH DISTINCT v\n")
                    .append("MATCH (s:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(v:VARIANT)\n")
                    .append("RETURN DISTINCT v.attr_chromosome AS ").append(NodeBuilder.CHROMOSOME).append(", v.attr_start AS ")
                    .append(NodeBuilder.START).append(", v.attr_reference AS ").append(NodeBuilder.REFERENCE)
                    .append(", v.attr_alternate AS ").append(NodeBuilder.ALTERNATE).append(", v.attr_type AS ").append(NodeBuilder.TYPE)
                    .append(", collect(s.id) AS ").append(NodeBuilder.SAMPLE).append(", collect(vc.attr_GT) AS ")
                    .append(NodeBuilder.GENOTYPE);
        } else {
            sb.append("WITH DISTINCT v\n")
                    .append("MATCH (s:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(v:VARIANT)\n")
                    .append("WITH DISTINCT v, collect(s.id) AS ").append(NodeBuilder.SAMPLE).append(", collect(vc.attr_GT) AS ")
                    .append(NodeBuilder.GENOTYPE).append("\n")
                    .append("MATCH (v:VARIANT)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__SO]-(so:SO)\n")
                    .append("RETURN DISTINCT v.attr_chromosome AS ").append(NodeBuilder.CHROMOSOME).append(", v.attr_start AS ")
                    .append(NodeBuilder.START).append(", v.attr_reference AS ").append(NodeBuilder.REFERENCE)
                    .append(", v.attr_alternate AS ").append(NodeBuilder.ALTERNATE).append(", v.attr_type AS ").append(NodeBuilder.TYPE)
                    .append(", ").append(NodeBuilder.SAMPLE).append(", ").append(NodeBuilder.GENOTYPE).append(", ct.attr_transcript AS ")
                    .append(NodeBuilder.TRANSCRIPT).append(", ct.attr_biotype AS ").append(NodeBuilder.BIOTYPE)
                    .append(", collect(so.name) AS ").append(NodeBuilder.CONSEQUENCE_TYPE).append(" ORDER BY ")
                    .append(NodeBuilder.CHROMOSOME).append(", ").append(NodeBuilder.START).append(", ").append(NodeBuilder.TRANSCRIPT);
        }
        return sb.toString();
    }

    public static List<CypherStatement> parsePanel(String panelValues, String biotypeValues, String chromWhere) {
        List<String> panels = Arrays.asList(panelValues.split(","));
        List<CypherStatement> cypherStatements = new ArrayList<>();


        // Match1
        String match = "MATCH (p:PANEL)-[:PANEL__GENE]-(:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)";

        // Where1
        String where = "WHERE " + getConditionString(panels, "p.name", false);

        // With1
        String with = "WITH DISTINCT tr1";

        cypherStatements.add(new CypherStatement(match, where, with));

        cypherStatements.add(getTranscriptMatch(biotypeValues, chromWhere));

        return cypherStatements;
    }

    public static List<CypherStatement> parseGene(String geneValues, String biotypeValues, String chromWhere) {
        List<String> genes = Arrays.asList(geneValues.split(","));
        List<CypherStatement> cypherStatements = new ArrayList<>();

        // Match1
        String match = "MATCH (r:XREF)-[:XREF]-(:GENE)-[:GENE__TRANSCRIPT]-(tr1:TRANSCRIPT)";

        // Where1
        String where = "WHERE " + getConditionString(genes, "r.id", false);

        // With1
        String with = "WITH DISTINCT tr1";

        cypherStatements.add(new CypherStatement(match, where, with));

        cypherStatements.add(getTranscriptMatch(biotypeValues, chromWhere));

        return cypherStatements;
    }

    private static CypherStatement getTranscriptMatch(String biotypeValues, String chromWhere) {
        // Match2
        String match = "MATCH (tr1:TRANSCRIPT)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(ct:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]"
                + "-(v:VARIANT)";

        // Where2
        String where = "";
        if (StringUtils.isNotEmpty(biotypeValues)) {
            where = "WHERE " + (getConditionString(Arrays.asList(biotypeValues.split(",")), "ct.attr_biotype", false)) + chromWhere;
        } else if (StringUtils.isNotEmpty(chromWhere)) {
            where = chromWhere.replace("AND", "WHERE");
        }

        // With2
        String with = "WITH DISTINCT v";

        return new CypherStatement(match, where, with);
    }

    private static List<CypherStatement> parseGenotype(String genotypeValues, String chromWhere) {
        List<CypherStatement> cypherStatements = new ArrayList<>();

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

                cypherStatements.add(new CypherStatement(match, where, with));
            }
        }
        return cypherStatements;
    }

    private static CypherStatement parseConsequenceType(String ctValues, String biotypeValues, String chromWhere) {
        List<String> cts = Arrays.asList(ctValues.split(","));

        // Match
        String match = "MATCH (so:SO)-[:CONSEQUENCE_TYPE__SO]-(ct:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-(v:VARIANT)";

        // Where
        String where = "WHERE " + getConditionString(cts, "so.name", false) + chromWhere;
        if (StringUtils.isNotEmpty(biotypeValues)) {
            where += (getConditionString(Arrays.asList(biotypeValues.split(",")), "ct.attr_biotype", true));
        }

        // With
        String with = "WITH DISTINCT v";

        return new CypherStatement(match, where, with);
    }

    private static CypherStatement parseBiotype(String biotypeValues, String chromWhere) {
        List<String> biotypes = Arrays.asList(biotypeValues.split(","));

        // Match
        String match = "MATCH (ct:CONSEQUENCE_TYPE)-[:VARIANT__CONSEQUENCE_TYPE]-(v:VARIANT)";

        // Where
        String where = "WHERE " + getConditionString(biotypes, "ct.attr_biotype", false) + chromWhere;

        // With
        String with = "WITH DISTINCT v";

        return new CypherStatement(match, where, with);
    }

    private static List<CypherStatement> parsePopulationFrequency(String popFreqValues, String chromWhere) {
        List<CypherStatement> cypherStatements = new ArrayList<>();

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

            cypherStatements.add(new CypherStatement(match, where.toString(), with));
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

                    cypherStatements.add(new CypherStatement(match, where, with));
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

    public static String getConditionString(List<String> stringList, String calling, String op, String logicalOp, boolean isNotFirst) {
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
}
