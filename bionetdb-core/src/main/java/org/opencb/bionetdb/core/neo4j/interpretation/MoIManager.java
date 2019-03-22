package org.opencb.bionetdb.core.neo4j.interpretation;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.NodeIterator;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.neo4j.query.Neo4JVariantQueryParam;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MoIManager {

    private NetworkDBAdaptor networkDBAdaptor;

    public MoIManager(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public List<Variant> getDominantVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.dominant(pedigree, disorder, false);
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.recessive(pedigree, disorder, false);
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getXLinkedDominantVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true);
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getXLinkedRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false);
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getYLinkedVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.yLinked(pedigree, disorder);
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "Y");
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    //------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //------------------------------------------------------------------

    private void putGenotypes(Query query, Map<String, List<String>> genotypes) {
        // genotype format: sample:0/1,1/1,....
        List<String> gt = new ArrayList<>();
        for (String sample : genotypes.keySet()) {
            gt.add(sample +  ":" + StringUtils.join(genotypes.get(sample), ","));
        }
        query.put("genotypes", gt);
    }

    private List<Variant> queryVariants(String cypher) throws BioNetDBException {
        List<Variant> variants = new ArrayList<>();

        NodeIterator nodeIterator = networkDBAdaptor.nodeIterator(cypher);
        while (nodeIterator.hasNext()) {
            variants.add(NodeBuilder.newVariant(nodeIterator.next()));
        }

        return variants;
    }
}
