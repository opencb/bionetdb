package org.opencb.bionetdb.lib.executors;

import org.opencb.bionetdb.core.models.network.NetworkStats;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;

public class NetworkQueryExecutor {

    private NetworkDBAdaptor networkDBAdaptor;

    public NetworkQueryExecutor(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public BioNetDBResult<NetworkStats> stats() {
        return networkDBAdaptor.networkStats();
    }
}
