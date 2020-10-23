package org.opencb.bionetdb.lib.utils;

import com.google.common.primitives.Longs;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Files;
import java.nio.file.Paths;

public class RocksDbManager {

    private int maxOpenFiles = -1;

    public RocksDbManager() {
        this(1000);
    }

    public RocksDbManager(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public RocksDB getDBConnection(String dbLocation, boolean forceCreate) {
        boolean indexingNeeded = forceCreate || !Files.exists(Paths.get(dbLocation));

        // A static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        // The OptionsFilter class contains a set of configurable DB options that determines the behavior of a database.
        Options options = new Options().setCreateIfMissing(true);
        if (maxOpenFiles > 0) {
            options.setMaxOpenFiles(maxOpenFiles);
        }

        RocksDB db;
        try {
            // A factory method that returns a RocksDB instance
            if (indexingNeeded) {
                db = RocksDB.open(options, dbLocation);
            } else {
                db = RocksDB.openReadOnly(options, dbLocation);
            }
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            db = null;
        }

        return db;
    }

    public boolean putString(String key, String value, RocksDB db) {
        try {
            // Add string value into the database
            db.put(key.getBytes(), value.getBytes());
            return true;
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            return false;
        }
    }

    public boolean putLong(String key, Long value, RocksDB db) {
        try {
            // Add boolean value into the database
            db.put(key.getBytes(), Longs.toByteArray(value));
            return true;
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            return false;
        }
    }

    public boolean putBoolean(String key, Boolean value, RocksDB db) {
        try {
            // Add boolean value into the database
            db.put(key.getBytes(), new byte[]{(byte) (value ? 1 : 0)});
            return true;
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            return false;
        }
    }

    public String getString(String key, RocksDB db) {
        try {
            // Get string value from the database
            byte[] value = db.get(key.getBytes());
            if (value == null) {
                return null;
            }
            return new String(value);
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            return null;
        }
    }

    public Long getLong(String key, RocksDB db) {
        try {
            // Get string value from the database
            byte[] value = db.get(key.getBytes());
            if (value == null) {
                return null;
            }
            return Longs.fromByteArray(value);
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            return null;
        }
    }


    public Boolean getBoolean(String key, RocksDB db) {
        try {
            // Get boolean value from the database
            byte[] value = db.get(key.getBytes());
            if (value == null) {
                return null;
            }
            return (value[0] == 1 ? true : false);
        } catch (RocksDBException e) {
            // Do some error handling
            e.printStackTrace();
            return null;
        }
    }

    public void close(RocksDB db) {
        db.close();
    }
}
