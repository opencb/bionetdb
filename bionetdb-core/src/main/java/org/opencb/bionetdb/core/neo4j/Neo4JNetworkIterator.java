package org.opencb.bionetdb.core.neo4j;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.opencb.bionetdb.core.api.NetworkIterator;
import org.opencb.bionetdb.core.models.Node;

public class Neo4JNetworkIterator extends NetworkIterator {
    private StatementResult statementResult;

    @Override
    public boolean hasNext() {
        return statementResult.hasNext();
    }

    @Override
    public Node next() {
        Record record = statementResult.next();
        // convert Record to Node ???
        return next();
    }

}
