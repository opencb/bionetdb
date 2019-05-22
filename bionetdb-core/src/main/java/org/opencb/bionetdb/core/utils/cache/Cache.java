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
import java.nio.file.Paths;

public abstract class Cache<T> {
    protected String objFilename;
    protected String xrefObjFilename;

    protected RocksDB objRocksDb;
    protected RocksDB xrefObjRocksDb;
    protected RocksDbManager rocksDbManager;

    protected ObjectMapper objMapper;
    protected ObjectReader objReader;

    private static Logger logger;

    public Cache(String objFilename, String xrefObjFilename) {
        this.objFilename = objFilename;
        this.xrefObjFilename = xrefObjFilename;

        rocksDbManager = new RocksDbManager();

        // Delete protein RocksDB files
        Paths.get(objFilename).toFile().delete();
        Paths.get(xrefObjFilename).toFile().delete();

        // Create gene RocksDB files (protein and xrefs)
        objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
        xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);

        objMapper = new ObjectMapper();
        objMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        logger = LoggerFactory.getLogger(this.getClass());
    }

//    public abstract void index(Path input, Path output) throws IOException;

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

    public void saveObject(String id, String json) {
        rocksDbManager.putString(id, json, objRocksDb);
    }

    public void saveXref(String xref, String id) {
        rocksDbManager.putString(xref, id, xrefObjRocksDb);
    }

    public RocksDB getObjRocksDb() {
        return objRocksDb;
    }

    public RocksDB getXrefObjRocksDb() {
        return xrefObjRocksDb;
    }

    public ObjectReader getObjReader() {
        return objReader;
    }
}
