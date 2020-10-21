package org.opencb.bionetdb.lib.db.iterators;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.opencb.bionetdb.core.utils.Neo4jConverter;
import org.opencb.bionetdb.lib.api.iterators.RowIterator;

import java.util.List;

public class Neo4JRowIterator implements RowIterator {
    private StatementResult statementResult;

    public Neo4JRowIterator(StatementResult statementResult) {
        this.statementResult = statementResult;
    }

    @Override
    public boolean hasNext() {
        return statementResult.hasNext();
    }

    @Override
    public List<Object> next() {
        Record record = statementResult.next();
        return Neo4jConverter.toObjectList(record);
    }

}
