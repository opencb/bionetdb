package org.opencb.bionetdb.core.api;

import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.Node;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by joaquin on 1/29/18.
 */
public interface NetworkDBManager extends AutoCloseable {

    void load(Path path) throws IOException, BioNetDBException;

    void load(Path path, QueryOptions queryOptions) throws IOException, BioNetDBException;

    QueryResult<Node> query(Query query, QueryOptions queryOptions) throws BioNetDBException;

    NetworkIterator iterator(Query query, QueryOptions queryOptions);

    void annotate();

    void annotateGenes(Query query, QueryOptions queryOptions);

    void annotateVariants(Query query, QueryOptions queryOptions);

    QueryResult getSummaryStats(Query query, QueryOptions queryOptions) throws BioNetDBException;

    void close() throws Exception;
}
