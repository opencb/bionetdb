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

    public static String parse(Query query, QueryOptions options) throws BioNetDBException {
        return parse("n", query, options);
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
