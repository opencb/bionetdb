package org.opencb.bionetdb.lib.db.query;

import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.api.query.NodeQuery;
import org.opencb.commons.datastore.core.QueryOptions;

public class Neo4JGeneQueryParser {

    public static String parse(NodeQuery query, QueryOptions options) throws BioNetDBException {
        // match (t:TRANSCRIPT)
        // with t match (t)-[]-(ct:VARIANT_CONSEQUENCE_TYPE) where ct.attr_biotype="protein_coding"
        // with t, ct match (ct)-[:SO_TERM]-(so:SO_TERM) where so.name="missense_variant"
        // with t match(t)-[:XREF]-(xref:SAMPLE) where xref.id = "HG00121"
        // return t

        StringBuilder cypher = new StringBuilder();

        return cypher.toString();
    }
}
