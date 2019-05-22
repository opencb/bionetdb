package org.opencb.bionetdb.app;

import org.junit.Test;

import static org.junit.Assert.*;

public class BioNetDBMainTest {

    @Test
    public void createCsvClinicalAnalysis() {
        String caPath = "/home/jtarraga/data150/clinicalAnalysis";
        String cmdLine = "~/appl/bionetdb/build/bin/bionetdb.sh create-csv -i " + caPath + "/input/ -o csv/ --clinical-analysis";
    }

}