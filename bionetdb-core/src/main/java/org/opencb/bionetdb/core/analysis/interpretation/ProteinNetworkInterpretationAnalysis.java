package org.opencb.bionetdb.core.analysis.interpretation;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;

public class ProteinNetworkInterpretationAnalysis {

    private NetworkDBAdaptor networkDBAdaptor;

    public ProteinNetworkInterpretationAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public QueryResult<Variant> execute(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi,
                                        boolean complexOrReaction, Query query) throws BioNetDBException {

        return networkDBAdaptor.proteinNetworkInterpretationAnalysis(pedigree, disorder, moi, complexOrReaction, query);
    }
}
