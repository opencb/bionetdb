package org.opencb.bionetdb.core.analysis.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.query.VariantQueryParam;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.*;

public class ProteinNetworkInterpretationAnalysis {

    private NetworkDBAdaptor networkDBAdaptor;

    public ProteinNetworkInterpretationAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public QueryResult<Variant> execute(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi,
                                        boolean complexOrReaction, Query query) throws BioNetDBException {
        // Check moi
        Map<String, List<String>> genotypes;
        switch (moi) {
            case MONOALLELIC:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.dominant(pedigree, disorder, false);
                break;
            case BIALLELIC:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.recessive(pedigree, disorder, false);
                break;
            case XLINKED_MONOALLELIC:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.xLinked(pedigree, disorder, true);
                break;
            case XLINKED_BIALLELIC:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.xLinked(pedigree, disorder, false);
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
            gt.add(sample + ":" + org.apache.commons.lang.StringUtils.join(genotypes.get(sample), ","));
        }
        query.put(VariantQueryParam.GENOTYPE.key(), gt);

        return networkDBAdaptor.proteinNetworkInterpretationAnalysis(complexOrReaction, query);
    }
}
