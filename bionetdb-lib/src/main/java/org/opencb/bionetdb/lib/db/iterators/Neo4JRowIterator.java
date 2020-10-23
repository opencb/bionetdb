package org.opencb.bionetdb.lib.db.iterators;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.opencb.bionetdb.lib.api.iterators.RowIterator;
import org.opencb.bionetdb.lib.utils.Neo4jConverter;

import java.util.List;

public class Neo4JRowIterator implements RowIterator {
    private Result result;

    public Neo4JRowIterator(Result result) {
        this.result = result;
    }

    @Override
    public boolean hasNext() {
        return result.hasNext();
    }

    @Override
    public List<Object> next() {
        Record record = result.next();
        return Neo4jConverter.toObjectList(record);
    }

}
