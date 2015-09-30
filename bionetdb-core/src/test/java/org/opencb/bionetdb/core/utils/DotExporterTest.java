package org.opencb.bionetdb.core.utils;

import org.junit.Test;
import org.opencb.bionetdb.core.models.Network;

import static org.junit.Assert.*;

/**
 * Created by imedina on 30/09/15.
 */
public class DotExporterTest {

    @Test
    public void testConvert() throws Exception {

        System.out.println(DotExporter.convert(new Network()));
    }
}