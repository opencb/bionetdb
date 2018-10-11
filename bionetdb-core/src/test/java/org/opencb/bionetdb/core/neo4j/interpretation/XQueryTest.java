package org.opencb.bionetdb.core.neo4j.interpretation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.pedigree.Individual;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class XQueryTest {

    private BioNetDbManager bioNetDbManager;
    private NetworkDBAdaptor networkDBAdaptor;

    private String database;

    @Before
    public void setUp() throws Exception {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig: bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }

            bioNetDBConfiguration.getDatabases().get(0).setPort(6660);
            bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(this.database, bioNetDBConfiguration, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        networkDBAdaptor.close();
    }

    @Test
    public void queryVariant() {

        Phenotype phenotype1 = new Phenotype("disease1", "disease1", "");
        Phenotype phenotype2 = new Phenotype("disease2", "disease2", "");
        Phenotype phenotype3 = new Phenotype("disease3", "disease2", "");
        Phenotype phenotype4 = new Phenotype("disease4", "disease2", "");

        Individual father = new Individual().setId("NA12877").setSex(Individual.Sex.MALE)
                .setPhenotypes(Arrays.asList(phenotype1, phenotype3));
        Individual mother = new Individual().setId("NA12878").setSex(Individual.Sex.FEMALE)
                .setPhenotypes(Collections.singletonList(phenotype2));
        Individual daughter = new Individual().setId("NA12879").setSex(Individual.Sex.FEMALE)
                .setPhenotypes(Collections.singletonList(phenotype2))
                .setMother(mother).setFather(father);
        Pedigree family1 = new Pedigree()
                .setMembers(Arrays.asList(father, mother, daughter))
                .setPhenotypes(Arrays.asList(phenotype1, phenotype2, phenotype3, phenotype4));
        family1.setProband(daughter);

        VariantFilter variantFilter = new VariantFilter((Arrays.asList("Hepatitis", "Anxiety")), Arrays.asList("AFR", "EUROPE"), 0.99,
                Arrays.asList("variant", "intron_variant"));
        Options options = new Options(true, false);

        bioNetDbManager.xQuery(family1, phenotype1, "dominant", Arrays.asList("CADM1", "BRCA1", "BRCA2", "TP53", "BCL2", "ADSL",
                "CTBP2P1", "BMPR2"), options);
    }
    // Arrays.asList("Hepatitis", "Anxiety")  Arrays.asList("AFR", "EUROPE")  "0.99"  Arrays.asList("variant", "intron_variant")
}
