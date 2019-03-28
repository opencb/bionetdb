package org.opencb.bionetdb.core.analysis;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.analysis.interpretation.ProteinNetworkInterpretationAnalysis;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;

public class InterpretationAnalysis extends BioNetDBAnalysis {

    public InterpretationAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        super(networkDBAdaptor);
    }

    public QueryResult<Variant> proteinNetworkAnalysis(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi,
                                                       boolean complexOrReaction, Query query) throws BioNetDBException {
        ProteinNetworkInterpretationAnalysis analysis = new ProteinNetworkInterpretationAnalysis(networkDBAdaptor);
        return analysis.execute(pedigree, disorder, moi, complexOrReaction, query);
    }
}
