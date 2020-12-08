package org.opencb.bionetdb.lib.analysis;

import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.analysis.interpretation.ProteinNetworkInterpretationAnalysis;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;

public class InterpretationAnalysis extends BioNetDBAnalysis {

    public InterpretationAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        super(networkDBAdaptor);
    }

    public DataResult<Variant> proteinNetworkAnalysis(Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi,
                                                      boolean complexOrReaction, Query query) throws BioNetDBException {
        ProteinNetworkInterpretationAnalysis analysis = new ProteinNetworkInterpretationAnalysis(networkDBAdaptor);
        return analysis.execute(pedigree, disorder, moi, complexOrReaction, query);
    }
}
