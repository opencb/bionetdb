package org.opencb.bionetdb.core.api;

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
public interface NetworkDBAdaptor {


    void insert(Network network);

    void insert(List<Interaction> interactionList);


    QueryResult get(Query query, QueryOptions queryOptions);

    QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions);


    QueryResult stats(Query query, QueryOptions queryOptions);


}
