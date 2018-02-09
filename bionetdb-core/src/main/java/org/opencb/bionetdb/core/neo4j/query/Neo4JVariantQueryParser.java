package org.opencb.bionetdb.core.neo4j.query;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.bionetdb.core.api.query.NodeQuery;
import org.opencb.bionetdb.core.api.query.VariantNodeQueryParam;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Neo4JVariantQueryParser {

    public static String parse(NodeQuery query, QueryOptions options) throws BioNetDBException {
        int counter = 1;

        // match (t:TRANSCRIPT)
        // with t match (t)-[]-(ct:CONSEQUENCE_TYPE) where ct.attr_biotype="protein_coding"
        // with t, ct match (ct)-[:SO]-(so:SO) where so.name="missense_variant"
        // with t match(t)-[:XREF]-(xref:SAMPLE) where xref.id = "HG00121"
        // return t

        StringBuilder cypher = new StringBuilder();
        List<String> whereStatement = new ArrayList<>();

        cypher.append("MATCH (n:").append(Node.Type.VARIANT).append(")");

        // Get IDs
        List<String> ids = new ArrayList<>();
        if (query.containsKey(VariantNodeQueryParam.ID.key())) {
            ids = query.getAsStringList(VariantNodeQueryParam.ID.key());
            whereStatement.add(StringUtils.join(ids, " OR "));
        }

        // Convert region string to region objects
        List<Region> regions = new ArrayList<>();
        if (query.containsKey(VariantNodeQueryParam.REGION.key())) {
            regions = Region.parseRegions(query.getString(VariantNodeQueryParam.REGION.key()));
            for (int i = 1; i < regions.size(); i++) {
                //whereStatement.add(" OR " + regions.get(i).toString() + ")")
            }
        }

        // Get genes
        List<String> genes = new ArrayList<>();
        if (query.containsKey(VariantNodeQueryParam.GENE.key())) {
            genes = query.getAsStringList(VariantNodeQueryParam.GENE.key());
        }

        // Get variant types
        List<String> types = new ArrayList<>();
        if (query.containsKey(VariantNodeQueryParam.TYPE.key())) {
            types = query.getAsStringList(VariantNodeQueryParam.TYPE.key());
        }

        // Output management
        List<String> output = Arrays.asList("node");
        if (query.containsKey(VariantNodeQueryParam.OUTPUT.key())) {
            output = query.getAsStringList(VariantNodeQueryParam.OUTPUT.key());
        }
        if (output.size() == 1 && output.get(0).equals("node")) {
            cypher.append("RETURN n");
        } else {
            cypher.append("RETURN n.").append(output.get(0));
            for (int i = 1; i < output.size(); i++) {
                cypher.append(", n.").append(output.get(i));
            }
        }


        return cypher.toString();
    }
}
