package org.opencb.bionetdb.core.neo4j.query;

import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.query.NetworkPathQuery;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;

import java.util.*;

/**
 * Created by imedina on 03/09/15.
 */
public class Neo4JQueryParser {

    //public static final Pattern operationPattern = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

    private static final Set<String> GENE_PSEUDO_ATTRS = new HashSet<>(Arrays.asList("drug", "hpo", "go"));
    private static final Set<String> VARIANT_PSEUDO_ATTRS = new HashSet<>(Arrays.asList("popFreq", "so"));

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

    public static String parse(Query query, QueryOptions options) {
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
}
