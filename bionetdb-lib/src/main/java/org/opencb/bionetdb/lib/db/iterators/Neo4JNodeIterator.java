package org.opencb.bionetdb.lib.db.iterators;

import org.neo4j.driver.Result;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.lib.api.iterators.NodeIterator;
import org.opencb.bionetdb.lib.utils.Neo4jConverter;

import java.util.ArrayList;
import java.util.List;

public class Neo4JNodeIterator implements NodeIterator {
    private Result result;
    private List<Node> buffer;

    public Neo4JNodeIterator(Result result) {
        this.result = result;
        this.buffer = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        if (!buffer.isEmpty()) {
            return true;
        } else {
            if (result.hasNext()) {
                buffer = Neo4jConverter.toNodeList(result.next());
            }
            return !buffer.isEmpty();
        }
    }

    @Override
    public Node next() {
        return buffer.remove(0);
    }

}
