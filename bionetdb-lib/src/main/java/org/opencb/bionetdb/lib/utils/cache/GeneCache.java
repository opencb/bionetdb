package org.opencb.bionetdb.lib.utils.cache;

import org.opencb.biodata.models.core.Gene;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class GeneCache extends Cache<Gene> {
    private static Logger logger;

    public GeneCache(Path indexPath) {
        super(indexPath + "/genes.rocksdb", indexPath + "/xref.genes.rocksdb");

        objReader = objMapper.reader(Gene.class);
        logger = LoggerFactory.getLogger(this.getClass());
    }
}
