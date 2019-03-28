package org.opencb.bionetdb.core.analysis;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;

public class BioNetDBAnalysis {

    protected NetworkDBAdaptor networkDBAdaptor;

    protected BioNetDBAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }
}
