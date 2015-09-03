package org.opencb.bionetdb.core.api;

import org.neo4j.graphdb.Node;
import org.opencb.bionetdb.core.models.Interaction;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.PhysicalEntity;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

import java.util.List;

/**
 * Created by imedina on 05/08/15.
 */
public interface NetworkDBAdaptor extends AutoCloseable {

    void insert(Network network, QueryOptions queryOptions);

    void addXrefs(String nodeID, List<Xref> xref_list);

    //TODO: To remove
    //public QueryResult getXrefs(String idNode);

    QueryResult get(Query query, QueryOptions queryOptions);

    QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions);


    QueryResult stats(Query query, QueryOptions queryOptions);

}
