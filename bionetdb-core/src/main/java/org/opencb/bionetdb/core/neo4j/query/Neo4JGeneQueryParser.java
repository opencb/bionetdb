package org.opencb.bionetdb.core.neo4j.query;

import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.QueryOptions;

public class Neo4JGeneQueryParser {

    public static String parse(NodeQuery query, QueryOptions options) throws BioNetDBException {
        // match (t:TRANSCRIPT)
        // with t match (t)-[]-(ct:CONSEQUENCE_TYPE) where ct.attr_biotype="protein_coding"
        // with t, ct match (ct)-[:SO]-(so:SO) where so.name="missense_variant"
        // with t match(t)-[:XREF]-(xref:SAMPLE) where xref.id = "HG00121"
        // return t

        StringBuilder cypher = new StringBuilder();

        return cypher.toString();
    }
}
