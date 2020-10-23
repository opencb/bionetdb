package org.opencb.bionetdb.lib.db.iterators;

import org.neo4j.driver.Result;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.lib.api.iterators.NetworkPathIterator;
import org.opencb.bionetdb.lib.utils.Neo4jConverter;

import java.util.ArrayList;
import java.util.List;

public class Neo4JNetworkPathIterator implements NetworkPathIterator {
    private Result result;
    private List<NetworkPath> buffer;

    public Neo4JNetworkPathIterator(Result result) {
        this.result = result;
        this.buffer = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        if (!buffer.isEmpty()) {
            return true;
        } else {
            if (result.hasNext()) {
                buffer = Neo4jConverter.toPathList(result.next());
            }
            return !buffer.isEmpty();
        }
    }

    @Override
    public NetworkPath next() {
        return buffer.remove(0);
    }
}
