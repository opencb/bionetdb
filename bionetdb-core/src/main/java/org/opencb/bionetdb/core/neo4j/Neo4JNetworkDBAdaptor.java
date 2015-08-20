package org.opencb.bionetdb.core.neo4j;

import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.models.Interaction;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
//import org.opencb.datastore.core.Query;
//import org.opencb.datastore.core.QueryOptions;
//import org.opencb.datastore.core.QueryResult;

import java.util.List;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private String database;

    public Neo4JNetworkDBAdaptor(String database) {
        this.database = database;
    }

    @Override
    public void insert(Network network, QueryOptions queryOptions) {

    }

    @Override
    public void insert(List<Interaction> interactionList, QueryOptions queryOptions) {

    }

    @Override
    public QueryResult get(Query query, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public QueryResult stats(Query query, QueryOptions queryOptions) {
        return null;
    }

}
