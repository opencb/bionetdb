package org.opencb.bionetdb.core.neo4j.interpretation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class PabloTieringTest {

    private BioNetDbManager bioNetDbManager;

    @Before
    public void setUp() throws Exception {
        try {
            BioNetDBConfiguration bioNetDBConfiguration = BioNetDBConfiguration.load(getClass().getResourceAsStream("/configuration.yml"));
            for (DatabaseConfiguration dbConfig : bioNetDBConfiguration.getDatabases()) {
                System.out.println(dbConfig);
            }

            bioNetDBConfiguration.getDatabases().get(0).setPort(17687);
            bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        bioNetDbManager.close();
    }

    @Test
    public void queryVariantPlatinum() {

        Disorder disorder = new Disorder("disease1", "disease1", "", "", null, null);

        Member healthyFather = new Member().setId("NA12877").setSex(Member.Sex.MALE);
        Member illFather = new Member().setId("NA12877").setSex(Member.Sex.MALE)
                .setDisorders(Collections.singletonList(disorder));

        Member healthyMother = new Member().setId("NA12878").setSex(Member.Sex.FEMALE);
        Member illMother = new Member().setId("NA12878").setSex(Member.Sex.FEMALE)
                .setDisorders(Collections.singletonList(disorder));

        Member healthyDaughter = new Member().setId("NA12879").setSex(Member.Sex.FEMALE)
                .setMother(illMother).setFather(healthyFather);
        Member illDaughter = new Member().setId("NA12879").setSex(Member.Sex.FEMALE)
                .setDisorders(Collections.singletonList(disorder))
                .setMother(illMother).setFather(healthyFather);

        Pedigree family1 = new Pedigree()
                .setMembers(Arrays.asList(healthyFather, illMother, illDaughter))
                .setDisorders(Collections.singletonList(disorder));
        family1.setProband(illDaughter);

        Pedigree family2 = new Pedigree()
                .setMembers(Arrays.asList(illFather, illMother, illDaughter))
                .setDisorders(Collections.singletonList(disorder));
        family2.setProband(illDaughter);

        Pedigree family3 = new Pedigree()
                .setMembers(Arrays.asList(illFather, healthyMother, healthyDaughter))
                .setDisorders(Collections.singletonList(disorder));
        family3.setProband(illDaughter);

//        T I E R I N G - P I E C E S
        QueryResult<Variant> dominantVariants = bioNetDbManager.getDominantVariants(family1, disorder, false,
                Collections.singletonList("CADM1"));
        System.out.println(dominantVariants.getResult() + "\n\n\n");

        QueryResult<Variant> recessiveVariants = bioNetDbManager.getRecessiveVariants(family1, disorder, false,
                Collections.singletonList("CADM1"));
        System.out.println(recessiveVariants.getResult() + "\n\n\n");

        QueryResult<Variant> xLinkedVariants = bioNetDbManager.getXLinkedVariants(family2, disorder, false,
                Collections.singletonList("FGF13"));
        System.out.println(xLinkedVariants.getResult() + "\n\n\n");

        QueryResult<Variant> xLinkedVariants2 = bioNetDbManager.getXLinkedVariants(family1, disorder, true,
                Collections.singletonList("CD99L2"));
        System.out.println(xLinkedVariants2.getResult() + "\n\n\n");

        QueryResult<Variant> yLinkedVariants = bioNetDbManager.getYLinkedVariants(family3, disorder, Collections.singletonList("DDX3Y"));
        System.out.println(yLinkedVariants.getResult() + "\n\n\n");
    }
}