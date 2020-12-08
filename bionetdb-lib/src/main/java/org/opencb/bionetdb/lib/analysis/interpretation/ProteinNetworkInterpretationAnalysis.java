package org.opencb.bionetdb.lib.analysis.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.query.VariantQueryParam;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;

import java.util.*;

import static org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance.COMPLETE;

public class ProteinNetworkInterpretationAnalysis {

    private NetworkDBAdaptor networkDBAdaptor;

    public ProteinNetworkInterpretationAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public DataResult<Variant> execute(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi,
                                       boolean complexOrReaction, Query query) throws BioNetDBException {
        // Check moi
        Map<String, List<String>> genotypes;
        switch (moi) {
            case AUTOSOMAL_DOMINANT:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.dominant(pedigree, disorder, COMPLETE);
                break;
            case AUTOSOMAL_RECESSIVE:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.recessive(pedigree, disorder, COMPLETE);
                break;
            case X_LINKED_DOMINANT:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.xLinked(pedigree, disorder, true, COMPLETE);
                break;
            case X_LINKED_RECESSIVE:
                genotypes = org.opencb.biodata.tools.pedigree.ModeOfInheritance.xLinked(pedigree, disorder, false, COMPLETE);
                break;
            case Y_LINKED:
                genotypes = ModeOfInheritance.yLinked(pedigree, disorder, COMPLETE);
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
