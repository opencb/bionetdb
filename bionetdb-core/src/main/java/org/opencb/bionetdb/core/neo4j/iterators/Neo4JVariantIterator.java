package org.opencb.bionetdb.core.neo4j.iterators;

import org.neo4j.driver.v1.Record;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.utils.converters.RecordToVariantConverter;

import java.util.Iterator;

public class Neo4JVariantIterator implements Iterator<Variant> {

    private final Iterator<Record> recordIterator;
    private final RecordToVariantConverter converter;

    public Neo4JVariantIterator(Iterator<Record> recordIterator) {
        this.recordIterator = recordIterator;
        converter = new RecordToVariantConverter();
    }

    @Override
    public boolean hasNext() {
        return recordIterator.hasNext();
    }

    @Override
    public Variant next() {
        Record next = recordIterator.next();
        return converter.convert(next);
    }
}
