package org.opencb.bionetdb.lib.analysis;

import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;

public class BioNetDBAnalysis {

    protected NetworkDBAdaptor networkDBAdaptor;

    protected BioNetDBAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }
}
