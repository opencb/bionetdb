package org.opencb.bionetdb.core.analysis;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;

public class NetworkAnalysis extends BioNetDBAnalysis {

    public NetworkAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }
}
