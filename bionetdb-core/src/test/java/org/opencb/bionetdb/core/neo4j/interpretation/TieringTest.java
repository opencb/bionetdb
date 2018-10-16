package org.opencb.bionetdb.core.neo4j.interpretation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TieringTest {

    private BioNetDbManager bioNetDbManager;
    private NetworkDBAdaptor networkDBAdaptor;

    private String database;

    @Before
    public void setUp() throws Exception {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig : bioNetDBConfiguration.getDatabases()) {
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

        Member father = new Member().setId("NA12877").setSex(Member.Sex.MALE)
                .setPhenotypes(Arrays.asList(phenotype1, phenotype3));
        Member mother = new Member().setId("NA12878").setSex(Member.Sex.FEMALE)
                .setPhenotypes(Collections.singletonList(phenotype2));
        Member daughter = new Member().setId("NA12879").setSex(Member.Sex.FEMALE)
                .setPhenotypes(Collections.singletonList(phenotype2))
                .setMother(mother).setFather(father);
        Pedigree family1 = new Pedigree()
                .setMembers(Arrays.asList(father, mother, daughter))
                .setPhenotypes(Arrays.asList(phenotype1, phenotype2, phenotype3, phenotype4));
        family1.setProband(daughter);

        List<QueryResult<Variant>> tieringVariants = bioNetDbManager.tiering(family1, phenotype1,
                Arrays.asList("CADM1", "FGF13", "CD99L2", "DDX3Y", "CDC27P2"), Collections.emptyList());
        for (QueryResult<Variant> variants : tieringVariants) {
            System.out.println("Query result: " + variants.getResult() + "\n\n");
        }
    }
}

// T I E R I N G - P I E C E S
//
//        QueryResult<Variant> dominantVariants = bioNetDbManager.getDominantVariants(family1, phenotype1, false,
//                Arrays.asList("CADM1", "CTBP2P1", "BRCA1"));
//        System.out.println(dominantVariants.getResult() + "\n\n\n");
//
//        QueryResult<Variant> recessiveVariants = bioNetDbManager.getRecessiveVariants(family1, phenotype1, false,
//                Collections.emptyList());
//        System.out.println(recessiveVariants.getResult() + "\n\n\n");
//
//        QueryResult<Variant> xLinkedVariants = bioNetDbManager.getXLinkedVariants(family1, phenotype1, false,
//                Arrays.asList("FGF13", "CD99L2"));
//        System.out.println(xLinkedVariants.getResult() + "\n\n\n");
//
//        QueryResult<Variant> xLinkedVariants2 = bioNetDbManager.getXLinkedVariants(family1, phenotype1, true,
//                Arrays.asList("FGF13", "CD99L2"));
//        System.out.println(xLinkedVariants2.getResult() + "\n\n\n");
//
//        QueryResult<Variant> yLinkedVariants = bioNetDbManager.getYLinkedVariants(family1, phenotype1,
//                Arrays.asList("DDX3Y", "CDC27P2"));
//        System.out.println(yLinkedVariants.getResult() + "\n\n\n");
//
//         QueryResult<Variant> deNovoVariants = bioNetDbManager.getDeNovoVariants(family1, Collections.emptyList(),
//              Collections.emptyList());
//         System.out.println(deNovoVariants.getResult() + "\n\n\n");
//
//        QueryResult<Variant> chVariants = bioNetDbManager.getCompoundHeterozygoteVariants(family1, Collections.singletonList("CADM1"),
//                Collections.emptyList());
//        System.out.println(chVariants.getResult() + "\n\n\n");


