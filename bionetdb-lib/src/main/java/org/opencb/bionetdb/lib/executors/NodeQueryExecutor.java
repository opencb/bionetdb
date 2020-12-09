package org.opencb.bionetdb.lib.executors;

import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.NodeStats;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.iterators.NodeIterator;
import org.opencb.bionetdb.lib.api.query.NodeQueryParam;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NodeQueryExecutor {

    private static final int QUERY_MAX_RESULTS = 50000;

    private NetworkDBAdaptor networkDBAdaptor;


    public NodeQueryExecutor(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public BioNetDBResult<Node> getNode(long uid) throws BioNetDBException {
        Query query = new Query();
        query.put(NodeQueryParam.UID.key(), uid);
//        query.put(NodeQueryParam.OUTPUT.key(), "node");
        return query(query, QueryOptions.empty());
    }

    public BioNetDBResult<Node> getNode(String id) throws BioNetDBException {
        Query query = new Query();
        query.put(NodeQueryParam.ID.key(), id);
//        query.put(NodeQueryParam.OUTPUT.key(), "node");
        return query(query, QueryOptions.empty());
    }

    public BioNetDBResult<Node> query(Query query, QueryOptions queryOptions) throws BioNetDBException {
        NodeIterator nodeIterator = iterator(query, queryOptions);
        return getNodeQueryResult(nodeIterator);
    }

    public BioNetDBResult<Node> query(String cypher) throws BioNetDBException {
        NodeIterator nodeIterator = iterator(cypher);
        return getNodeQueryResult(nodeIterator);
    }

    public BioNetDBResult<NodeStats> stats() {
        return stats(new Query());
    }

    public BioNetDBResult<NodeStats> stats(Query query) {
        return networkDBAdaptor.nodeStats(query);
    }

    public NodeIterator iterator(Query query, QueryOptions queryOptions) throws BioNetDBException {
        return networkDBAdaptor.nodeIterator(query, queryOptions);
    }

    public NodeIterator iterator(String cypher) throws BioNetDBException {
        return networkDBAdaptor.nodeIterator(cypher);
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private BioNetDBResult<Node> getNodeQueryResult(NodeIterator nodeIterator) {
        List<Node> nodes = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (nodeIterator.hasNext()) {
            if (nodes.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            nodes.add(nodeIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new BioNetDBResult(time, Collections.emptyList(), nodes.size(), nodes, nodes.size());
    }
}
