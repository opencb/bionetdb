package org.opencb.bionetdb.core.neo4j;

import org.neo4j.driver.v1.StatementResult;
import org.opencb.bionetdb.core.api.NodeIterator;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.core.utils.Neo4JConverter;

import java.util.ArrayList;
import java.util.List;

public class Neo4JNodeIterator implements NodeIterator {
    private StatementResult statementResult;
    private List<Node> buffer;

    public Neo4JNodeIterator(StatementResult statementResult) {
        this.statementResult = statementResult;
        this.buffer = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        if (!buffer.isEmpty()) {
            return true;
        } else {
            if (statementResult.hasNext()) {
                buffer = Neo4JConverter.toNodes(statementResult.next());
            }
            return !buffer.isEmpty();
        }
    }

    @Override
    public Node next() {
        return buffer.remove(0);
    }

}
