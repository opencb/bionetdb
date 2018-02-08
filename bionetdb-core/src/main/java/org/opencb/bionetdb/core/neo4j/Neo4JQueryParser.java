package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 03/09/15.
 */
public class Neo4JQueryParser {

    //public static final Pattern operationPattern = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

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

    public static String parse(Query srcQuery, Query destQuery, QueryOptions options) {
        return null;
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
