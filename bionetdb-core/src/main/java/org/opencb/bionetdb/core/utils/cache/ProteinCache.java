package org.opencb.bionetdb.core.utils.cache;

import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class ProteinCache extends Cache<Entry> {

    protected static Logger logger;

    public ProteinCache(Path indexPath) {
        super(indexPath + "/proteins.rocksdb", indexPath + "/xref.proteins.rocksdb");

        objReader = objMapper.reader(Entry.class);
        logger = LoggerFactory.getLogger(this.getClass());
    }
}
