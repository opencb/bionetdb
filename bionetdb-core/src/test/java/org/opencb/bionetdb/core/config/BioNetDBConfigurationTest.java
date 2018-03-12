package org.opencb.bionetdb.core.config;

import org.junit.Test;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by imedina on 05/10/15.
 */
public class BioNetDBConfigurationTest {

    @Test
    public void testSerialize() throws Exception {

        BioNetDBConfiguration bioNetDBConfiguration = new BioNetDBConfiguration();

        bioNetDBConfiguration.setDefaultDatabase("testDB");

        List<DatabaseConfiguration> databases = new ArrayList<>();
        databases.add(new DatabaseConfiguration("testDB",  null));

        bioNetDBConfiguration.setDatabases(databases);

        bioNetDBConfiguration.serialize(new FileOutputStream("/tmp/bionetdb-configuration-test.yml"));
    }
}