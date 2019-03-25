package org.opencb.bionetdb.core.neo4j.interpretation.ProteinSystem;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JVariantIterator;
import org.opencb.bionetdb.core.neo4j.query.Neo4JVariantQueryParam;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.*;

import static org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser.*;
import static org.opencb.bionetdb.core.utils.NodeBuilder.*;

public class ProteinSystemAnalysis {

    private NetworkDBAdaptor networkDBAdaptor;

    public ProteinSystemAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public List<Variant> execute(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi, boolean complexOrReaction,
                                 Query query) throws BioNetDBException {
        // Check moi
        Map<String, List<String>> genotypes;
        switch (moi) {
            case MONOALLELIC:
                genotypes = ModeOfInheritance.dominant(pedigree, disorder, false);
                break;
            case BIALLELIC:
                genotypes = ModeOfInheritance.recessive(pedigree, disorder, false);
                break;
            case XLINKED_MONOALLELIC:
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true);
                break;
            case XLINKED_BIALLELIC:
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false);
                break;
            case YLINKED:
                genotypes = ModeOfInheritance.yLinked(pedigree, disorder);
                break;
            default:
                genotypes = new HashMap<>();
                genotypes.put(pedigree.getProband().getId(), Collections.singletonList("NON_REF"));
                break;
        }
        // yLinked or other mistakes can return empty genotype lists. The next exception aims to avoid those errors.
        genotypes.entrySet().removeIf((entry) -> CollectionUtils.isEmpty(entry.getValue()));
        if (genotypes.size() == 0) {
            throw new IllegalArgumentException("Number of individuals with filled genotypes list is zero");
        }
        List<String> gt = new ArrayList<>();
        for (String sample : genotypes.keySet()) {
            gt.add(sample + ":" + StringUtils.join(genotypes.get(sample), ","));
        }
        query.put(Neo4JVariantQueryParam.GENOTYPE.key(), gt);

        // Create cypher statement from query
        String cypher = parse(query, QueryOptions.empty(), complexOrReaction);

        // Execute cypher query and convert to variants
        Driver driver = ((Neo4JNetworkDBAdaptor) networkDBAdaptor).getDriver();
        Session session = driver.session();
        StatementResult result = session.run(cypher);
        session.close();

        List<Variant> variants = new ArrayList<>();
        while (result.hasNext()) {
            variants.add(new Neo4JVariantIterator(result).next());
        }

        // Return variants
        return variants;
    }

    private String parse(Query query, QueryOptions options, boolean complexOrReaction) {
        StringBuilder cypher = new StringBuilder();

        if (!query.containsKey(Neo4JVariantQueryParam.PANEL.key())) {
            throw new IllegalArgumentException("Missing panels");
        }

        String nexus = complexOrReaction ? COMPLEX : REACTION;

        cypher.append("MATCH (v:VARIANT)-[:VARIANT__CONSEQUENCE_TYPE]-(ct:CONSEQUENCE_TYPE)-[:CONSEQUENCE_TYPE__TRANSCRIPT]-(:TRANSCRIPT)");
                cypher.append("-[:TRANSCRIPT__PROTEIN]-(prot1:PROTEIN)-");
        if (complexOrReaction) {
            cypher.append("[:COMPONENT_OF_COMPLEX]-(nex:COMPLEX)-[:COMPONENT_OF_COMPLEX]-");
        } else {
            cypher.append("[:REACTANT|:PRODUCT]-(nex:REACTION)-[:REACTANT|:PRODUCT]-");
        }
        cypher.append("(prot2:PROTEIN)-[:TRANSCRIPT__PROTEIN]-(tr2:TRANSCRIPT)-[:GENE__TRANSCRIPT]-(g:GENE)-[:PANEL__GENE]-(p:PANEL)\n");

        cypher.append("WHERE ").append(getConditionString(Arrays.asList(query.getString(Neo4JVariantQueryParam.PANEL.key())
                .split(",")), "p.name", false))
                .append(getConditionString(Arrays.asList(query.getString(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key())
                        .split(",")), "ct.attr_biotype", true)).append("\n");

        cypher.append("WITH DISTINCT v, prot1.name AS ").append(TARGET_PROTEIN).append(", nex.name AS ").append(nexus)
                .append(", prot2.name AS ").append(PANEL_PROTEIN).append(", g.id AS ").append(PANEL_GENE).append("\n");

        String systemParams = ", " + TARGET_PROTEIN + ", " + nexus + ", " + PANEL_PROTEIN + ", " + PANEL_GENE;

        query.remove(Neo4JVariantQueryParam.PANEL.key());
        query.remove(Neo4JVariantQueryParam.ANNOT_BIOTYPE.key());
        List<CypherStatement> cypherStatements = getCypherStatements(query, options);

        int i = 0;
        CypherStatement st;
        for (i = 0; i < cypherStatements.size() - 1; i++) {
            st = cypherStatements.get(i);
            cypher.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append(st.getWith()).append(systemParams)
                    .append("\n");
        }
        st = cypherStatements.get(i);
        cypher.append(st.getMatch()).append("\n").append(st.getWhere()).append("\n").append("WITH DISTINCT v").append(systemParams)
                .append("\n").append("MATCH (s:SAMPLE)-[:SAMPLE__VARIANT_CALL]-(vc:VARIANT_CALL)-[:VARIANT__VARIANT_CALL]-(v:VARIANT)")
                .append("\n").append("RETURN DISTINCT v.attr_chromosome AS ").append(NodeBuilder.CHROMOSOME).append(", v.attr_start AS ")
                .append(NodeBuilder.START).append(", v.attr_reference AS ").append(NodeBuilder.REFERENCE).append(", v.attr_alternate AS ")
                .append(NodeBuilder.ALTERNATE).append(", v.attr_type AS ").append(NodeBuilder.TYPE)
                .append(", collect(s.id), collect(vc.attr_GT)").append(systemParams);

        System.out.println(cypher);
        return cypher.toString();
    }
}
