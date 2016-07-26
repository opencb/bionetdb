package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 03/09/15.
 */
public class Neo4JQueryParser {

    //public static final Pattern operationPattern = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

    public static String parse2(Query n, Object obj, Query m, QueryOptions options) throws BioNetDBException {

        // query --n id:CHK1;cl:nucleoplasm --m id:P40343;cl:cytosol [--neighbours 4] [--relationship XREF]
        // --n id:XXX,cl:XXX,on:XXX


        String nFilter = parse("n", n, options);
        String mFilter = parse("m", m, options);

// add ()--()

//        // First we construct the Match
//        if (query.get(NetworkDBAdaptor.NetworkQueryParams.INT_TYPE.key()) != null
//                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.INT_TYPE.key()).isEmpty()) {
//            StringBuilder relationsCypher = new StringBuilder().append(":");
//            for (String interaction : (List<String>) query.get(NetworkDBAdaptor.NetworkQueryParams.INT_TYPE.key())) {
//                // TODO: Probably, in this point, I would have to check the proper Interaction name given the id...
//                relationsCypher.append(interaction).append("|");
//            }
//            relationsCypher.setLength(relationsCypher.length() - 1);
//
//            if (query.get(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()) != null
//                    && !query.getString(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()).isEmpty()) {
//                relationsCypher.append("*..").append(query.getInt(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()));
//            }
//
//            cypherQuery.append("[").append(relationsCypher).append("]");
//        } else {
//            if (query.get(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()) != null
//                    && !query.getString(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()).isEmpty()) {
//                cypherQuery.append("[*..").append(query.getInt(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key())).append("]");
//            }
//        }
        return null;
    }

    public static String parse(Query query, QueryOptions options) throws BioNetDBException {
        return parse("n", query, options);
    }

    public static String parse(String nodeName, Query query, QueryOptions options) throws BioNetDBException {
        if (query == null) {
            query = new Query();
        }

        StringBuilder cypherQuery = new StringBuilder();

        String node = "(" + nodeName;
        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.TYPE.key()))) {
            node += ":" + query.getString(NetworkDBAdaptor.NetworkQueryParams.TYPE.key().replace(',', '|')).toUpperCase();
        }
        node += ")";

        List<String> myMatchList = new ArrayList<>();
        List<String> myWhereClauses = new ArrayList<>();
        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()))) {
            myMatchList.add(node + "-[:XREF]->(" + nodeName + "x:XREF)");
            myWhereClauses.add(nodeName + "x.id IN [\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key())
                    .replace(",", "\", \"") + "\"]");
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()).isEmpty()) {
            myMatchList.add(node + "-[:ONTOLOGY]->(" + nodeName + "o:ONTOLOGY)");
            myWhereClauses.add(nodeName + "o.id IN [\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key())
                    .replace(",", "\", \"") + "\"]");
        }

        if (StringUtils.isNotEmpty(query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key()))) {
            myMatchList.add(node + "-[:CELLULAR_LOCATION]->(" + nodeName + "c:CELLULAR_LOCATION)");
            myWhereClauses.add(nodeName + "c.id IN [\"" + query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key())
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
