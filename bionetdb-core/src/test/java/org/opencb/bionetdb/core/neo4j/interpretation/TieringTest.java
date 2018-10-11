package org.opencb.bionetdb.core.neo4j.interpretation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.pedigree.Individual;
import org.opencb.biodata.models.core.pedigree.Pedigree;
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
        Pedigree family1;
        Pedigree family2;
        Phenotype phenotype1;
        Phenotype phenotype2;
        Phenotype phenotype3;
        Phenotype phenotype4;

        phenotype1 = new Phenotype("disease1", "disease1", "");
        phenotype2 = new Phenotype("disease2", "disease2", "");
        phenotype3 = new Phenotype("disease3", "disease2", "");
        phenotype4 = new Phenotype("disease4", "disease2", "");

        Individual father = new Individual().setId("NA12877").setSex(Individual.Sex.MALE)
                .setPhenotypes(Arrays.asList(phenotype1, phenotype3));
        Individual mother = new Individual().setId("NA12878").setSex(Individual.Sex.FEMALE)
                .setPhenotypes(Collections.singletonList(phenotype2));
        Individual daughter = new Individual().setId("NA12879").setSex(Individual.Sex.FEMALE)
                .setPhenotypes(Collections.singletonList(phenotype2))
                .setMother(mother).setFather(father);
        family1 = new Pedigree()
                .setMembers(Arrays.asList(father, mother, daughter))
                .setPhenotypes(Arrays.asList(phenotype1, phenotype2, phenotype3, phenotype4));
        family1.setProband(daughter);

        List<QueryResult<Variant>> tieringVariants = bioNetDbManager.tiering(family1, phenotype1,
                Arrays.asList("CADM1", "FGF13", "CD99L2", "DDX3Y", "CDC27P2"), Collections.emptyList());
        for (QueryResult<Variant> variants : tieringVariants) {
            System.out.println("Query result: " + variants.getResult() + "\n\n");

        }
        // Arrays.asList("Hepatitis", "Anxiety")  Arrays.asList("AFR", "EUROPE")  "0.99"  Arrays.asList("variant", "intron_variant")
    }
}

// A L T E R N A T I V E - P E D I G R E E
//
//        // This family is not storaged in Neo4J, that is why it is not used on testing.
//        Individual ind1 = new Individual().setId("ind1").setSex(Individual.Sex.FEMALE)
//                .setPhenotypes(Collections.singletonList(phenotype1));
//        Individual ind2 = new Individual().setId("ind2").setSex(Individual.Sex.MALE);
//        Individual ind3 = new Individual().setId("ind3").setSex(Individual.Sex.MALE);
//        Individual ind4 = new Individual().setId("ind4").setSex(Individual.Sex.FEMALE)
//                .setPhenotypes(Collections.singletonList(phenotype1));
//        Individual ind5 = new Individual().setId("ind5").setSex(Individual.Sex.MALE)
//                .setMother(ind1).setFather(ind2);
//        Individual ind6 = new Individual().setId("ind6").setSex(Individual.Sex.FEMALE)
//                .setMother(ind1).setFather(ind2);
//        Individual ind7 = new Individual().setId("ind7").setSex(Individual.Sex.MALE)
//                .setPhenotypes(Collections.singletonList(phenotype1))
//                .setMother(ind4).setFather(ind3);
//        Individual ind8 = new Individual().setId("ind8").setSex(Individual.Sex.MALE)
//                .setMother(ind4).setFather(ind3);
//        Individual ind9 = new Individual().setId("ind9").setSex(Individual.Sex.FEMALE);
//        Individual ind10 = new Individual().setId("ind10").setSex(Individual.Sex.FEMALE)
//                .setPhenotypes(Collections.singletonList(phenotype1));
//        Individual ind11 = new Individual().setId("ind11").setSex(Individual.Sex.MALE)
//                .setPhenotypes(Collections.singletonList(phenotype1))
//                .setMother(ind6).setFather(ind7);
//        Individual ind12 = new Individual().setId("ind12").setSex(Individual.Sex.FEMALE)
//                .setMother(ind6).setFather(ind7);
//        Individual ind13 = new Individual().setId("ind13").setSex(Individual.Sex.MALE)
//                .setMother(ind6).setFather(ind7);
//        Individual ind14 = new Individual().setId("ind14").setSex(Individual.Sex.FEMALE)
//                .setMother(ind9).setFather(ind8);
//        Individual ind15 = new Individual().setId("ind15").setSex(Individual.Sex.MALE)
//                .setPhenotypes(Collections.singletonList(phenotype1))
//                .setMother(ind9).setFather(ind8);
//        Individual ind16 = new Individual().setId("ind16").setSex(Individual.Sex.FEMALE)
//                .setPhenotypes(Collections.singletonList(phenotype1))
//                .setMother(ind10).setFather(ind11);
//        Individual ind17 = new Individual().setId("ind17").setSex(Individual.Sex.MALE)
//                .setMother(ind10).setFather(ind11);
//        Individual ind18 = new Individual().setId("ind18").setSex(Individual.Sex.MALE)
//                .setPhenotypes(Collections.singletonList(phenotype1));
//        family2 = new Pedigree()
//                .setMembers(Arrays.asList(ind1, ind2, ind3, ind4, ind5, ind6, ind7, ind8, ind9, ind10, ind11, ind12, ind13, ind14, ind15,
//                        ind16, ind17, ind18))
//                .setPhenotypes(Collections.singletonList(phenotype1));

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


