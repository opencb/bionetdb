package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void test1() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        Variant variant = new VariantBuilder().setChromosome("X").setStart(10000).setEnd(20000).setAlternate("A").setReference("C").build();

        List<ConsequenceType> cts = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ConsequenceType ct = new ConsequenceType();
            ct.setBiotype("protein_coding");
            ct.setEnsemblGeneId("ENSG00000" + i);
            ct.setEnsemblTranscriptId("ENST00000" + i);
            List<SequenceOntologyTerm> sos = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                SequenceOntologyTerm so = new SequenceOntologyTerm();
                so.setAccession("SO:000" + j);
                so.setName("so_name_" + j);
                sos.add(so);
            }
            ct.setSequenceOntologyTerms(sos);

            cts.add(ct);
        }
        variant.setAnnotation(new VariantAnnotation());
        variant.getAnnotation().setConsequenceTypes(cts);

        System.out.println("\nVariant:");
        System.out.println(variant.toJson());

        String compress = Utils.compress(variant, objectMapper);
        Variant variant1 = Utils.uncompress(compress, Variant.class, objectMapper);

        System.out.println("\nVariant (after compressing and uncompressing):");
        System.out.println(variant1.toJson());

        String ctCompress = Utils.compress(variant.getAnnotation().getConsequenceTypes(), objectMapper);
        List<ConsequenceType> cts1 = Utils.uncompress(ctCompress, variant.getAnnotation().getConsequenceTypes().getClass(), objectMapper);
        System.out.println(cts1);
    }

    @Test
    public void test2() {
        String cmdline = "import --delimiter=\"" + StringEscapeUtils.escapeJava(CsvInfo.SEPARATOR) + "\" --output";
        System.out.println(cmdline);
    }

}