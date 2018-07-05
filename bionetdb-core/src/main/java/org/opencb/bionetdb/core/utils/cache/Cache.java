package org.opencb.bionetdb.core.utils.cache;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.utils.RocksDbManager;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public abstract class Cache<T> {
    protected ObjectMapper objMapper;
    protected ObjectReader objReader;

    protected RocksDB objRocksDb;
    protected RocksDB xrefObjRocksDb;
    protected RocksDbManager rocksDbManager;

    private static Logger logger;

    public Cache() {
        objMapper = new ObjectMapper();
        objMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        rocksDbManager = new RocksDbManager();

        logger = LoggerFactory.getLogger(this.getClass());
    }

    public abstract void index(Path input, Path output) throws IOException;

    public String getPrimaryId(String id) {
        return rocksDbManager.getString(id, xrefObjRocksDb);
    }

    public void addXrefId(String xrefId, String primaryId) {
        rocksDbManager.putString(xrefId, primaryId, xrefObjRocksDb);
    }

    public T get(String id) {
        T obj = null;

        String primaryId = getPrimaryId(id);
        if (StringUtils.isNotEmpty(primaryId)) {
            try {
                obj = objReader.readValue(rocksDbManager.getString(primaryId, objRocksDb));
            } catch (IOException e) {
                logger.info("Error parsing object with ID {}: {}", id, primaryId);
                logger.info(e.getMessage());
                obj = null;
            }
        }

        return obj;
    }
}
