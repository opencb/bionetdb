package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.commons.lang.StringEscapeUtils;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.databind.node.JsonNodeType.POJO;
import static org.junit.Assert.*;

public class UtilsTest {

    ObjectMapper objectMapper;

    @Before
    public void init() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

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

        Class<? extends List> aClass = variant.getAnnotation().getConsequenceTypes().getClass();
        String ctCompress = Utils.compress(variant.getAnnotation().getConsequenceTypes(), objectMapper);
//        List<ConsequenceType> cts1 = Utils.uncompress(ctCompress, variant.getAnnotation().getConsequenceTypes().getClass(), objectMapper);

        List<ConsequenceType> cts1 = Utils.uncompress(ctCompress, new ArrayList<ConsequenceType>().getClass(), objectMapper);

        System.out.println(cts1);
    }

    @Test
    public void test2() {
        String cmdline = "import --delimiter=\"" + StringEscapeUtils.escapeJava(CsvInfo.SEPARATOR) + "\" --output";
        System.out.println(cmdline);
    }

    @Test
    public void test3() throws IOException {
        String compress = "\"H4sIAAAAAAAAAJVWTW/bMAz9L74uB5H6tG9t16W7BAUS7DIMg+uoiQFHzmQnWFH0v492uqZJ6zC52eLjo0g9UXxOlottk2TPL6Mk+kcffSh8kiXjZJS0T+vuczr5QT8hX3nC/fw1SvKq9THkbWe8IlM5pw/IUGojUAmRjbNuuWk38/K/jw8EekN0xph3S8kXQuYh1G3elnWgjSTFMtaruqlXHT/0RHlsD7yPd3q8o78E6AI/7/Z2d58JIcBa2bHVm9g7Ltd18jI6gAjnWIhSahByO5nORA+yKADe43xo/OqhmlHaTRHLdfuBWBrHxDZG6VOxx31sAJNq+0nssQ/+mBJTcZJyl440xohz03nz0or88LIiuFRwB4AaOQiFZiBoNRcIIU1ZMZzY7uz65va98W48uTkmUFICEwNQcdugi8GxCAmGhWhOgM5KhgUAkS2sBS4QpIK7iM6llguUAgdxQp/OCHodcyzaDmty3xQcohSX3QchuAys1Zw+6JLz+jAci3NquFT7JIVB8Vn3GU4SNLCXQFlevuwxCYdsqfiCk8CHW2ZsDDhJJTjQw/xhOrk/5MGuSw3zvF5sUh+Xk7EcRKNjIcZx9VVoueIBOlZnkA6f0l5ERisjLxIRArDdGiWnEKkF05qAjoRL0kHKNThqBqxajeMeOiWRg2gE7i3Uin0LAVmIlpYLJNUZj5DiSifPqK5U3C0mNXxkoUF1XjbrKn+6qUlwfzbdjDnbjcFlaGMdfm/zWOahJdfiEPI66K7r9abqZ9lvcWd9m4F7h7h9HXS7lQVNZCTpsr1qmroojyxf42bxPdBkmxd7w+Mm9H95NS3q6LtFGt3PmJkrHxbtMsng5R/Wgyo/9AsAAA==\"";
        Variant variant = Utils.uncompress(compress, Variant.class, objectMapper);
        System.out.println(variant.toJson());
    }

    @Test
    public void test4() throws IOException {
        String compressedStudy = "\"H4sIAAAAAAAAAJWTXU/CMBSG/0uvsXZdWzauHCJKggoMvSHGHGHq4tiWrSMhhP/uOY2IBC5kN33P9+mTbrZh9byokpp1Zi8tVifzIl9AtY4ym1Q52F3gvaiWYFGz2ymjRNss1oMF67DPjxU0dVJdlRnYNG+WnTTLmmWaw+vOw7A+zVyrjVOu0FPKDzEE1lbpW+NGbVh0TSF0302a/EfejyeQf8XYCG0eBD6jTXEbtoIqhdyi3R8MpzcTdI2iOHY1qDX1j/qoBNcoe0lWO0PQACizwq7LJCYAzm2MpnldqJM/Iy8wX9PM8VM0RLstQy7a4veT1HmEAdVGNUlgMSrqg3LPD2iRB7QouR+T4FqEblFBs1HFXcr2jeKSsuZNVlYp3bJPFxr36Paauw2fx/HwkRxS8lAGim23rUO0yjsbLS3aVv9la8R5bEP8jtmi39d7tJ4wlLkDqs0poIL70j/iqblR5gRPJTFfHfBElDueAQ/1AU+Ph0YGyJMeOSxLfLY9sIBPd8bEpcC3T6e3P93PAJYIY9E3P+3f4VADAAA=\"";
        List<StudyEntry> studies = Utils.uncompressList(compressedStudy, StudyEntry.class, objectMapper);

        Variant variant = new VariantBuilder().setChromosome("X").setStart(1000).setReference("A").setAlternate("C").setStudyId("222").build();
        variant.setStudies(studies);
        System.out.println(variant.getStudies().get(0).getFileId());
    }
}