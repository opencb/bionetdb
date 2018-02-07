package org.opencb.bionetdb.core.neo4j;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.opencb.bionetdb.core.api.RowIterator;
import org.opencb.bionetdb.core.utils.RecordConverter;

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
        return RecordConverter.toRow(record);
    }

}
