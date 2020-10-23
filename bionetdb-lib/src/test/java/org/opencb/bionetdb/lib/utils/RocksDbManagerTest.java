package org.opencb.bionetdb.lib.utils;

import org.junit.Test;
import org.rocksdb.RocksDB;

public class RocksDbManagerTest {

    @Test
    public void test1() {
        RocksDbManager rocksDbManager = new RocksDbManager(1000);

        String dbLocation = "/tmp/rocksdb";
        RocksDB db = rocksDbManager.getDBConnection(dbLocation, true);
        rocksDbManager.putString("hello", "word", db);
        rocksDbManager.putBoolean("rs123", true, db);
        rocksDbManager.putBoolean("rs111", false, db);

        String key = "toto";
        System.out.println(key + " --> " + rocksDbManager.getString(key, db));
        key = "hello";
        System.out.println(key + " --> " + rocksDbManager.getString(key, db));


        key = "rs321";
        System.out.println(key + " --> " + rocksDbManager.getBoolean(key, db));
        key = "rs123";
        System.out.println(key + " --> " + rocksDbManager.getBoolean(key, db));
        key = "rs111";
        System.out.println(key + " --> " + rocksDbManager.getBoolean(key, db));

        rocksDbManager.close(db);
    }

}