package org.opencb.bionetdb.core.utils;

import org.junit.Test;
import org.rocksdb.RocksDB;

public class RocksDBManagerTest {

    @Test
    public void test1() {
        RocksDBManager rocksDBManager = new RocksDBManager(1000);

        String dbLocation = "/tmp/rocksdb";
        RocksDB db = rocksDBManager.getDBConnection(dbLocation, true);
        rocksDBManager.putString("hello", "word", db);
        rocksDBManager.putBoolean("rs123", true, db);
        rocksDBManager.putBoolean("rs111", false, db);

        String key = "toto";
        System.out.println(key + " --> " + rocksDBManager.getString(key, db));
        key = "hello";
        System.out.println(key + " --> " + rocksDBManager.getString(key, db));


        key = "rs321";
        System.out.println(key + " --> " + rocksDBManager.getBoolean(key, db));
        key = "rs123";
        System.out.println(key + " --> " + rocksDBManager.getBoolean(key, db));
        key = "rs111";
        System.out.println(key + " --> " + rocksDBManager.getBoolean(key, db));

        rocksDBManager.close(db);
    }

}