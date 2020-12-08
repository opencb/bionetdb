package org.opencb.bionetdb.lib.analysis;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.iterators.NodeIterator;
import org.opencb.bionetdb.lib.api.query.VariantQueryParam;
import org.opencb.bionetdb.lib.utils.NodeBuilder;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance.COMPLETE;

public class VariantAnalysis extends BioNetDBAnalysis {

    public VariantAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        super(networkDBAdaptor);
    }

    public DataResult<Variant> getDominantVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException, IOException {
        Map<String, List<String>> genotypes = ModeOfInheritance.dominant(pedigree, disorder, COMPLETE);
        putGenotypes(query, genotypes);

        return null;
//        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public DataResult<Variant> getRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.recessive(pedigree, disorder, COMPLETE);
        putGenotypes(query, genotypes);

        return null;
//        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public DataResult<Variant> getXLinkedDominantVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true, COMPLETE);
        query.put(VariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        return null;
//        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public DataResult<Variant> getXLinkedRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false, COMPLETE);
        query.put(VariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        return null;
//        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public DataResult<Variant> getYLinkedVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.yLinked(pedigree, disorder, COMPLETE);
        query.put(VariantQueryParam.CHROMOSOME.key(), "Y");
        putGenotypes(query, genotypes);

        return null;
//        return networkDBAdaptor.variantQuery(query, QueryOptions.empty());
    }

    public DataResult<Variant> getDeNovoVariants(Pedigree pedigree, Query query) throws BioNetDBException, IOException {
        Map<String, List<String>> genotypes = ModeOfInheritance.deNovo(pedigree);
        putGenotypes(query, genotypes);
        query.put(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);

        // Get variants and genotypes
        return null;
//        DataResult<Variant> variantQueryResult = networkDBAdaptor.variantQuery(query, QueryOptions.empty());
//        List<Variant> variants = variantQueryResult.getResults();
//
//        int dbTime = 0;
//        if (CollectionUtils.isEmpty(variants)) {
//            return new DataResult<>(dbTime, new ArrayList<>(), 0, Collections.emptyList(), 0);
//        } else {
//            List<String> sampleNames = Arrays.asList(variants.get(0).getAnnotation().getAdditionalAttributes()
//                    .get("samples").getAttribute().get(NodeBuilder.SAMPLE).split(","));
//
//            // Filter deNovo variants
//            int probandSampleIdx = sampleNames.indexOf(pedigree.getProband().getId());
//            int motherSampleIdx = sampleNames.indexOf(pedigree.getProband().getMother().getId());
//            int fatherSampleIdx = sampleNames.indexOf(pedigree.getProband().getFather().getId());
//            List<Variant> deNovoVariants = ModeOfInheritance.deNovo(variants.iterator(), probandSampleIdx, motherSampleIdx,
//                    fatherSampleIdx);
//
//            return new DataResult<>(dbTime, Collections.emptyList(), deNovoVariants.size(), deNovoVariants, deNovoVariants.size());
//        }
    }

    public DataResult<Map<String, List<Variant>>> getCompoundHeterozygousVariants(Pedigree pedigree, Query query)
            throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.compoundHeterozygous(pedigree);
        putGenotypes(query, genotypes);

        query.put(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);
        query.put(VariantQueryParam.INCLUDE_CONSEQUENCE_TYPE.key(), true);

        // Get variants and genotypes
        return null;
//        DataResult<Variant> variantQueryResult = networkDBAdaptor.variantQuery(query, QueryOptions.empty());
//        List<Variant> variants = variantQueryResult.getResults();
//
//        int dbTime = 0;
//        if (CollectionUtils.isEmpty(variants)) {
//            return new DataResult<>(dbTime, Collections.emptyList(), 0, Collections.emptyList(), 0);
//        } else {
//
//            List<String> sampleNames = Arrays.asList(variants.get(0).getAnnotation().getAdditionalAttributes().get("samples")
// .getAttribute()
//                    .get(NodeBuilder.SAMPLE).split(","));
//
//            // Filter deNovo variants
//            int probandSampleIdx = sampleNames.indexOf(pedigree.getProband().getId());
//            int motherSampleIdx = sampleNames.indexOf(pedigree.getProband().getMother().getId());
//            int fatherSampleIdx = sampleNames.indexOf(pedigree.getProband().getFather().getId());
//            Map<String, List<Variant>> chVariants = ModeOfInheritance.compoundHeterozygous(variants.iterator(), probandSampleIdx,
//                    motherSampleIdx, fatherSampleIdx);
//
//            return new DataResult<>(dbTime, Collections.emptyList(), chVariants.size(), Collections.singletonList(chVariants),
//                    chVariants.size());
//        }
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
