package org.opencb.bionetdb.core.neo4j;

import org.neo4j.driver.v1.StatementResult;
import org.opencb.bionetdb.core.api.PathIterator;
import org.opencb.bionetdb.core.network.Path;
import org.opencb.bionetdb.core.utils.Neo4JConverter;

import java.util.ArrayList;
import java.util.List;

public class Neo4JPathIterator implements PathIterator {
    private StatementResult statementResult;
    private List<Path> buffer;

    public Neo4JPathIterator(StatementResult statementResult) {
        this.statementResult = statementResult;
        this.buffer = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        if (!buffer.isEmpty()) {
            return true;
        } else {
            if (statementResult.hasNext()) {
                buffer = Neo4JConverter.toPath(statementResult.next());
            }
            return !buffer.isEmpty();
        }
    }

    @Override
    public Path next() {
        return buffer.remove(0);
    }
}
