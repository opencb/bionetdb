package org.opencb.bionetdb.core.analysis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.iterators.NodeIterator;
import org.opencb.bionetdb.core.api.query.VariantQueryParam;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.IOException;
import java.util.*;

public class VariantAnalysis extends BioNetDBAnalysis {

    public VariantAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        super(networkDBAdaptor);
    }

    public QueryResult<Variant> getDominantVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException, IOException {
        Map<String, List<String>> genotypes = ModeOfInheritance.dominant(pedigree, disorder, false);
        putGenotypes(query, genotypes);

        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public QueryResult<Variant> getRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.recessive(pedigree, disorder, false);
        putGenotypes(query, genotypes);

        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public QueryResult<Variant> getXLinkedDominantVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true);
        query.put(VariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public QueryResult<Variant> getXLinkedRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false);
        query.put(VariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public QueryResult<Variant> getYLinkedVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.yLinked(pedigree, disorder);
        query.put(VariantQueryParam.CHROMOSOME.key(), "Y");
        putGenotypes(query, genotypes);

        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public QueryResult<Variant> getDeNovoVariants(Pedigree pedigree, Query query) throws BioNetDBException, IOException {
        Map<String, List<String>> genotypes = ModeOfInheritance.deNovo(pedigree);
        putGenotypes(query, genotypes);
        query.put(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);

        // Get variants and genotypes
        QueryResult<Variant> variantQueryResult = networkDBAdaptor.variantQuery(query, QueryOptions.empty());
        List<Variant> variants = variantQueryResult.getResult();

        int dbTime = 0;
        if (CollectionUtils.isEmpty(variants)) {
            return new QueryResult<>("", dbTime, 0, 0, "", "", Collections.emptyList());
        } else {
            List<String> sampleNames = Arrays.asList(variants.get(0).getAnnotation().getAdditionalAttributes()
                    .get("samples").getAttribute().get(NodeBuilder.SAMPLE).split(","));

            // Filter deNovo variants
            int probandSampleIdx = sampleNames.indexOf(pedigree.getProband().getId());
            int motherSampleIdx = sampleNames.indexOf(pedigree.getProband().getMother().getId());
            int fatherSampleIdx = sampleNames.indexOf(pedigree.getProband().getFather().getId());
            List<Variant> deNovoVariants = ModeOfInheritance.deNovo(variants.iterator(), probandSampleIdx, motherSampleIdx,
                    fatherSampleIdx);

            return new QueryResult<>("", dbTime, variants.size(), variants.size(), "", "", deNovoVariants);
        }
    }

    public QueryResult<Map<String, List<Variant>>> getCompoundHeterozygousVariants(Pedigree pedigree, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.compoundHeterozygous(pedigree);
        putGenotypes(query, genotypes);

        query.put(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);
        query.put(VariantQueryParam.INCLUDE_CONSEQUENCE_TYPE.key(), true);

        // Get variants and genotypes
        QueryResult<Variant> variantQueryResult = networkDBAdaptor.variantQuery(query, QueryOptions.empty());
        List<Variant> variants = variantQueryResult.getResult();

        int dbTime = 0;
        if (CollectionUtils.isEmpty(variants)) {
            return new QueryResult<>("", dbTime, 0, 0, "", "", Collections.emptyList());
        } else {

            List<String> sampleNames = Arrays.asList(variants.get(0).getAnnotation().getAdditionalAttributes().get("samples").getAttribute()
                    .get(NodeBuilder.SAMPLE).split(","));

            // Filter deNovo variants
            int probandSampleIdx = sampleNames.indexOf(pedigree.getProband().getId());
            int motherSampleIdx = sampleNames.indexOf(pedigree.getProband().getMother().getId());
            int fatherSampleIdx = sampleNames.indexOf(pedigree.getProband().getFather().getId());
            Map<String, List<Variant>> chVariants = ModeOfInheritance.compoundHeterozygous(variants.iterator(), probandSampleIdx,
                    motherSampleIdx, fatherSampleIdx);

            return new QueryResult<>("", dbTime, variants.size(), variants.size(), "", "",
                    Collections.singletonList(chVariants));
        }
    }

    //------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //------------------------------------------------------------------

    private void putGenotypes(Query query, Map<String, List<String>> genotypes) {
        // genotype format: sample:0/1,1/1,....
        List<String> gt = new ArrayList<>();
        for (String sample : genotypes.keySet()) {
            gt.add(sample + ":" + StringUtils.join(genotypes.get(sample), ","));
        }
        query.put(VariantQueryParam.GENOTYPE.key(), gt);
    }

    private List<Variant> queryNodes(String cypher) throws BioNetDBException {
        List<Variant> nodes = new ArrayList<>();

        NodeIterator nodeIterator = networkDBAdaptor.nodeIterator(cypher);
        while (nodeIterator.hasNext()) {
            nodes.add(NodeBuilder.newVariant(nodeIterator.next()));
        }

        return nodes;
    }
}
