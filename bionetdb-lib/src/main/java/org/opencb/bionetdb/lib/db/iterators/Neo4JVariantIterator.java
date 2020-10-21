package org.opencb.bionetdb.lib.db.iterators;

import org.neo4j.driver.v1.StatementResult;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.lib.api.iterators.VariantIterator;
import org.opencb.bionetdb.lib.db.converters.Neo4JRecordToVariantConverter;

public class Neo4JVariantIterator implements VariantIterator {

    private StatementResult statementResult;
    private Neo4JRecordToVariantConverter converter;

    public Neo4JVariantIterator(StatementResult statementResult) {
        this.statementResult = statementResult;
        this.converter = new Neo4JRecordToVariantConverter();
    }

    @Override
    public boolean hasNext() {
        return statementResult.hasNext();
    }

    @Override
    public Variant next() {
        return converter.convert(statementResult.next());
    }
}
