package org.opencb.bionetdb.core;

import org.junit.Before;
import org.junit.Test;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;

import java.io.IOException;
import java.nio.file.Paths;

public class BioNetDBManagerTest {

    private String database = "scerevisiae";
    private BioNetDBManager bioNetDBManager;

    @Before
    public void initialize () {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig: bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }
             bioNetDBManager = new BioNetDBManager(database, bioNetDBConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        clientConfiguration.setVersion("v4");
//        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
//        cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
    }

    @Test
    public void insertVCF() throws BioNetDBException {
        String vcfFilename = "/home/jtarraga/data150/vcf/2.vcf";
        bioNetDBManager.loadVcf(Paths.get(vcfFilename));
    }

}