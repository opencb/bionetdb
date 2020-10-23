package org.opencb.bionetdb.lib.db.iterators;

import org.neo4j.driver.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.lib.api.iterators.VariantIterator;
import org.opencb.bionetdb.lib.db.converters.Neo4JRecordToVariantConverter;

public class Neo4JVariantIterator implements VariantIterator {

    private Result result;
    private Neo4JRecordToVariantConverter converter;

    public Neo4JVariantIterator(Result result) {
        this.result = result;
        this.converter = new Neo4JRecordToVariantConverter();
    }

    @Override
    public boolean hasNext() {
        return result.hasNext();
    }

    @Override
    public Variant next() {
        return converter.convert(result.next());
    }
}
