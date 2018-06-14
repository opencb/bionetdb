package org.opencb.bionetdb.core.neo4j;

import org.neo4j.driver.v1.StatementResult;
import org.opencb.bionetdb.core.api.NetworkPathIterator;
import org.opencb.bionetdb.core.network.NetworkPath;
import org.opencb.bionetdb.core.utils.Neo4jConverter;

import java.util.ArrayList;
import java.util.List;

public class Neo4JNetworkPathIterator implements NetworkPathIterator {
    private StatementResult statementResult;
    private List<NetworkPath> buffer;

    public Neo4JNetworkPathIterator(StatementResult statementResult) {
        this.statementResult = statementResult;
        this.buffer = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        if (!buffer.isEmpty()) {
            return true;
        } else {
            if (statementResult.hasNext()) {
                buffer = Neo4jConverter.toPathList(statementResult.next());
            }
            return !buffer.isEmpty();
        }
    }

    @Override
    public NetworkPath next() {
        return buffer.remove(0);
    }
}
