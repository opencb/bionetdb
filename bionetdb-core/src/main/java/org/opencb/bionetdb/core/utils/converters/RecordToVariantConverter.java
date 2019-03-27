package org.opencb.bionetdb.core.utils.converters;

import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Record;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.Converter;
import org.opencb.bionetdb.core.utils.NodeBuilder;

import java.util.*;

public class RecordToVariantConverter implements Converter<Record, Variant> {

    @Override
    public Variant convert(Record record) {
        VariantBuilder variantBuilder = Variant.newBuilder();

        if (!record.get(NodeBuilder.CHROMOSOME).isNull()) {
            variantBuilder.setChromosome(record.get(NodeBuilder.CHROMOSOME).asString());
        }

        if (!record.get(NodeBuilder.START).isNull()) {
            variantBuilder.setStart(Integer.parseInt(record.get(NodeBuilder.START).asString()));
        }

        if (!record.get(NodeBuilder.END).isNull()) {
            variantBuilder.setStart(Integer.parseInt(record.get(NodeBuilder.END).asString()));
        }

        if (!record.get(NodeBuilder.REFERENCE).isNull()) {
            variantBuilder.setReference(record.get(NodeBuilder.REFERENCE).asString());
        }

        if (!record.get(NodeBuilder.ALTERNATE).isNull()) {
            variantBuilder.setAlternate(record.get(NodeBuilder.ALTERNATE).asString());
        }

        if (!record.get(NodeBuilder.TYPE).isNull()) {
            variantBuilder.setType(VariantType.valueOf(record.get(NodeBuilder.TYPE).asString()));
        }

        variantBuilder.setStudyId("S");
        variantBuilder.setFormat("GT");

        Variant variant = variantBuilder.build();


        // Add consequence type, biotype and transcript info
        ConsequenceType ct = new ConsequenceType();
        if (!record.get(NodeBuilder.TRANSCRIPT).isNull()) {
            ct.setEnsemblTranscriptId(NodeBuilder.TRANSCRIPT);
        }

        if (!record.get(NodeBuilder.BIOTYPE).isNull()) {
            ct.setBiotype(NodeBuilder.BIOTYPE);
        }

        if (!record.get(NodeBuilder.CONSEQUENCE_TYPE).isNull()) {
            List<SequenceOntologyTerm> seqOntList = new ArrayList<>();
            SequenceOntologyTerm seqOnt = new SequenceOntologyTerm();
            for (Object so : record.get(NodeBuilder.CONSEQUENCE_TYPE).asList()) {
                seqOnt.setName(so.toString());
                seqOntList.add(seqOnt);
            }
            ct.setSequenceOntologyTerms(seqOntList);
        }

        // Add protein system info in annotation additional attributes
        Map<String, String> systemMap = new HashMap<>();

        if (!record.get(NodeBuilder.TARGET_PROTEIN).isNull()) {
            systemMap.put(NodeBuilder.TARGET_PROTEIN, record.get(NodeBuilder.TARGET_PROTEIN).asString());
        }

        if (!record.get(NodeBuilder.COMPLEX).isNull()) {
            systemMap.put(NodeBuilder.COMPLEX, record.get(NodeBuilder.COMPLEX).asString());
        }

        if (!record.get(NodeBuilder.REACTION).isNull()) {
            systemMap.put(NodeBuilder.REACTION, record.get(NodeBuilder.REACTION).asString());
        }

        if (!record.get(NodeBuilder.PANEL_PROTEIN).isNull()) {
            systemMap.put(NodeBuilder.PANEL_PROTEIN, record.get(NodeBuilder.PANEL_PROTEIN).asString());
        }

        if (!record.get(NodeBuilder.PANEL_GENE).isNull()) {
            systemMap.put(NodeBuilder.PANEL_GENE, record.get(NodeBuilder.PANEL_GENE).asString());
        }

        // Add genotype info in annotation additional attributes
        Map<String, String> sampleMap = new HashMap<>();
        List<String> sampleNames = new ArrayList<>();
        String sampleString;

        if (!record.get(NodeBuilder.SAMPLE).isNull()) {
            for (Object sample : record.get(NodeBuilder.SAMPLE).asList()) {
                sampleNames.add(sample.toString());
            }
            sampleString = StringUtils.join(sampleNames, ",");
            sampleMap.put(NodeBuilder.SAMPLE, sampleString);
        }

        List<List<String>> sampleGT = new ArrayList<>();

        if (!record.get(NodeBuilder.GENOTYPE).isNull()) {
            for (Object gt : record.get(NodeBuilder.GENOTYPE).asList()) {
                sampleGT.add(Collections.singletonList(gt.toString()));
            }
        }

        AdditionalAttribute proteinSystemAttribute = new AdditionalAttribute();
        proteinSystemAttribute.setAttribute(systemMap);

        AdditionalAttribute samplesAttribute = new AdditionalAttribute();
        samplesAttribute.setAttribute(sampleMap);

        Map<String, AdditionalAttribute> additionalAttributes = new HashMap<>();
        additionalAttributes.put("proteinSystem", proteinSystemAttribute);
        additionalAttributes.put("samples", samplesAttribute);

        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setAdditionalAttributes(additionalAttributes);

        variant.setAnnotation(variantAnnotation);
        variant.getStudies().get(0).setSamplesData(sampleGT);
        variant.getAnnotation().setConsequenceTypes(Collections.singletonList(ct));

        return variant;
    }
}
