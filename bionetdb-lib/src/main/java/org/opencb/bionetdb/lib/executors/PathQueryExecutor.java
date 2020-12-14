package org.opencb.bionetdb.lib.executors;

import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.iterators.NetworkPathIterator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PathQueryExecutor {

    private static final int QUERY_MAX_RESULTS = 50000;

    private NetworkDBAdaptor networkDBAdaptor;


    public PathQueryExecutor(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public BioNetDBResult<NetworkPath> query(Query query, QueryOptions queryOptions) throws BioNetDBException {
        NetworkPathIterator pathIterator = iterator(query, queryOptions);
        return getNetworkPathQueryResult(pathIterator);
    }

    public BioNetDBResult<NetworkPath> query(String cypher) throws BioNetDBException {
        NetworkPathIterator nodeIterator = iterator(cypher);
        return getNetworkPathQueryResult(nodeIterator);
    }

    public NetworkPathIterator iterator(Query query, QueryOptions queryOptions) throws BioNetDBException {
        return networkDBAdaptor.networkPathIterator(query, queryOptions);
    }

    public NetworkPathIterator iterator(String cypher) throws BioNetDBException {
        return networkDBAdaptor.networkPathIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private BioNetDBResult<NetworkPath> getNetworkPathQueryResult(NetworkPathIterator pathIterator) {
        List<NetworkPath> networkPaths = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (pathIterator.hasNext()) {
            if (networkPaths.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            networkPaths.add(pathIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new BioNetDBResult(time, Collections.emptyList(), networkPaths.size(), networkPaths, networkPaths.size());
    }
}
