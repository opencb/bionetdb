package org.opencb.bionetdb.core.neo4j;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 03/09/15.
 */
public class Neo4JQueryParser {

    //public static final Pattern operationPattern = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

    public static String parse(Query query, QueryOptions options) throws BioNetDBException {
/*
        final String AND = ";";
        final String OR = ",";
        final String IS = ":";
*/
        StringBuilder cypherQuery = new StringBuilder();

        /*
        a, b -> Physical Entities
        c, d -> Xrefs
        e, f -> Ontology
        g, h -> CellularLocation
         */

        cypherQuery.append("MATCH p=(a:PhysicalEntity)-");
        // First we construct the Match
        if (query.get(NetworkDBAdaptor.NetworkQueryParams.INT_TYPE.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.INT_TYPE.key()).isEmpty()) {
            StringBuilder relationsCypher = new StringBuilder().append(":");
            for (String interaction : (List<String>) query.get(NetworkDBAdaptor.NetworkQueryParams.INT_TYPE.key())) {
                // TODO: Probably, in this point, I would have to check the proper Interaction name given the id...
                relationsCypher.append(interaction).append("|");
            }
            relationsCypher.setLength(relationsCypher.length() - 1);

            if (query.get(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()) != null
                    && !query.getString(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()).isEmpty()) {
                relationsCypher.append("*..").append(query.getInt(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()));
            }

            cypherQuery.append("[").append(relationsCypher).append("]");
        } else {
            if (query.get(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()) != null
                    && !query.getString(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key()).isEmpty()) {
                cypherQuery.append("[*..").append(query.getInt(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key())).append("]");
            }
        }
        cypherQuery.append("-(b:PhysicalEntity)");

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()).isEmpty()) {
            // I have Xref ID's to filter by, so we need to add them to the match beforehand
            cypherQuery.append(", (a)-[:XREF]->(c:Xref), (b)-[:XREF]->(d:Xref)");
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()).isEmpty()) {
            // I have Ontology attributes to filter by, so we need to add them to the match beforehand
            cypherQuery.append(", (a)-[:ONTOLOGY]->(e:Ontology), (b)-[:ONTOLOGY]->(f:Ontology)");
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key()).isEmpty()) {
            // I have Location attributes to filter by, so we need to add them to the match beforehand
            cypherQuery.append(", (a)-[:CELLULARLOCATION]->(g:CellularLocation), (b)-[:CELLULARLOCATION]->(h:CellularLocation)");
        }


        // Begins the WHERE clause
        List<StringBuilder> myWhereClauses = new ArrayList<>();
        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()).isEmpty()) {
            StringBuilder whereClause = new StringBuilder();
            StringBuilder aux = new StringBuilder();
            for (String myID : query.getAsStringList(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key())) {
                aux.append("\"").append(myID).append("\", ");
            }
            aux.setLength(aux.length() - 2);
            whereClause.append(" (c.id IN [").append(aux).append("] AND d.id IN [").append(aux).append("]) ");
            myWhereClauses.add(whereClause);
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_DESCRIPTION.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_DESCRIPTION.key()).isEmpty()) {
            // TODO: The description field would be better if we implement a like comparison...
            StringBuilder whereClause = new StringBuilder();
            StringBuilder aux = new StringBuilder();
            for (String myID : query.getAsStringList(NetworkDBAdaptor.NetworkQueryParams.PE_DESCRIPTION.key())) {
                aux.append("\"").append(myID).append("\", ");
            }
            aux.setLength(aux.length() - 2);
            whereClause.append(" (a.description IN [").append(aux).append("] AND b.description IN [").append(aux).append("]) ");
            myWhereClauses.add(whereClause);
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_TYPE.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_TYPE.key()).isEmpty()) {
            StringBuilder whereClause = new StringBuilder();
            StringBuilder aux = new StringBuilder();
            for (String myID : query.getAsStringList(NetworkDBAdaptor.NetworkQueryParams.PE_TYPE.key())) {
                aux.append("\"").append(myID).append("\", ");
            }
            aux.setLength(aux.length() - 2);
            whereClause.append(" (a.type IN [").append(aux).append("] AND b.type IN [").append(aux).append("]) ");
            myWhereClauses.add(whereClause);
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()).isEmpty()) {
            StringBuilder whereClause = new StringBuilder();
            StringBuilder aux = new StringBuilder();
            for (String myID : query.getAsStringList(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key())) {
                aux.append("\"").append(myID).append("\", ");
            }
            aux.setLength(aux.length() - 2);
            whereClause.append(" (e.id IN [").append(aux).append("] AND f.id IN [").append(aux).append("]) ");
            myWhereClauses.add(whereClause);
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key()).isEmpty()) {
            StringBuilder whereClause = new StringBuilder();
            StringBuilder aux = new StringBuilder();
            for (String myID : query.getAsStringList(NetworkDBAdaptor.NetworkQueryParams.PE_CELLOCATION.key())) {
                aux.append("\"").append(myID).append("\", ");
            }
            aux.setLength(aux.length() - 2);
            whereClause.append(" (g.id IN [").append(aux).append("] AND h.id IN [").append(aux).append("]) ");
            myWhereClauses.add(whereClause);
        }

        if (myWhereClauses.size() == 0) {
            throw new BioNetDBException("Incomplete query. A match clause must always be followed by a where clause.");
        }

        cypherQuery.append(" WHERE ");
        for (StringBuilder whereClause : myWhereClauses) {
            cypherQuery.append(whereClause).append("AND");
        }
        cypherQuery.setLength(cypherQuery.length() - 3);


        // Return clause
        // TODO: At the moment, we are always going to return the main path. However, we should change
        // TODO: this depending on the arguments.
        cypherQuery.append(" RETURN p");

        return cypherQuery.toString();
    }

/*        cypherQuery.append("MATCH p=(m:Xref)<-[:XREF]-(:PhysicalEntity)-[:REACTANT*1..2]-(:PhysicalEntity)-[:XREF]->(n:Xref) ");

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key()).isEmpty()) {
            StringBuilder idBuilder = new StringBuilder("[");
            for (String myID : (List<String>) query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key())) {
                idBuilder.append("\"" + myID + "\", ");
            }
            idBuilder.setLength(idBuilder.length() - 2);
            idBuilder.append("]");
            cypherQuery.append("WHERE m.id IN " + idBuilder + " AND n.id IN " + idBuilder + " ");
        }

        cypherQuery.append("RETURN p;");
        */




        /*
        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_TYPE.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_TYPE.key()).isEmpty()) {
            cypherQuery.append("MATCH (" + query.get(NetworkDBAdaptor.NetworkQueryParams.PE_TYPE.key()) + ") -[:XREF] - (NODE)");
            cypherQuery.append("");
        }

        if (query.get(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()) != null
                && !query.getString(NetworkDBAdaptor.NetworkQueryParams.PE_ONTOLOGY.key()).isEmpty()) {
            System.out.println("no implemented yet");
        }
        */
}
