package org.opencb.bionetdb.core.neo4j.converters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Record;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.Converter;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.bionetdb.core.utils.Utils;

import java.io.IOException;
import java.util.*;

public class Neo4JRecordToVariantConverter implements Converter<Record, Variant> {

    private ObjectMapper objMapper;

    public Neo4JRecordToVariantConverter() {
        // Object mapper
        objMapper = new ObjectMapper();
        objMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }


    @Override
    public Variant convert(Record record) {
        try {
            List<List<String>> sampleGT = new ArrayList<>();
            Map<String, String> sampleMap = new HashMap<>();
            Map<String, AdditionalAttribute> additionalAttributes = new HashMap<>();

            // Create variant
            Variant variant = Utils.uncompress(fixString(record.get("attr_core").asString()), Variant.class, objMapper);

            for (String attr : record.keys()) {
                if (record.get(attr) != null) {
                    attr = attr.replace("attr_", "");
                    switch (attr) {
                        case "studies":
                            List<org.opencb.biodata.models.variant.StudyEntry> studies =
                                    Utils.uncompressList(fixString(record.get(attr).asString()), StudyEntry.class, objMapper);
                            variant.setStudies(studies);
                            break;
                        case "consequenceTypes":
                            List<ConsequenceType> ct = Utils.uncompressList(fixString(record.get(attr).asString()), ConsequenceType.class,
                                    objMapper);
                            variant.getAnnotation().setConsequenceTypes(ct);
                            break;
                        case "xrefs":
                            List<Xref> xrefs = Utils.uncompressList(fixString(record.get(attr).asString()), Xref.class, objMapper);
                            variant.getAnnotation().setXrefs(xrefs);
                            break;
                        case "populationFrequencies":
                            List<PopulationFrequency> popFreqs = Utils.uncompressList(fixString(record.get(attr).asString()),
                                    PopulationFrequency.class, objMapper);
                            variant.getAnnotation().setPopulationFrequencies(popFreqs);
                            break;
                        case "conservation":
                            List<Score> conservation = Utils.uncompressList(fixString(record.get(attr).asString()), Score.class, objMapper);
                            variant.getAnnotation().setConservation(conservation);
                            break;
                        case "geneExpression":
                            List<Expression> expression = Utils.uncompressList(fixString(record.get(attr).asString()), Expression.class,
                                    objMapper);
                            variant.getAnnotation().setGeneExpression(expression);
                            break;
                        case "geneTraitAssociation":
                            List<GeneTraitAssociation> gta = Utils.uncompressList(fixString(record.get(attr).asString()),
                                    GeneTraitAssociation.class, objMapper);
                            variant.getAnnotation().setGeneTraitAssociation(gta);
                            break;
                        case "geneDrugInteraction":
                            List<GeneDrugInteraction> gdi = Utils.uncompressList(fixString(record.get(attr).asString()),
                                    GeneDrugInteraction.class, objMapper);
                            variant.getAnnotation().setGeneDrugInteraction(gdi);
                            break;
                        case "variantTraitAssociation":
                            VariantTraitAssociation vta = Utils.uncompress(fixString(record.get(attr).asString()),
                                    VariantTraitAssociation.class, objMapper);
                            variant.getAnnotation().setVariantTraitAssociation(vta);
                            break;
                        case "traitAssociation":
                            List<EvidenceEntry> ta = Utils.uncompressList(fixString(record.get(attr).asString()), EvidenceEntry.class,
                                    objMapper);
                            variant.getAnnotation().setTraitAssociation(ta);
                            break;
                        case "functionalScore":
                            List<Score> fs = Utils.uncompressList(fixString(record.get(attr).asString()), Score.class, objMapper);
                            variant.getAnnotation().setFunctionalScore(fs);
                            break;
                        case NodeBuilder.SAMPLE:
                            if (!record.get(NodeBuilder.SAMPLE).isNull()) {
                                List<String> sampleNames = new ArrayList<>();
                                String sampleString;
                                for (Object sample : record.get(NodeBuilder.SAMPLE).asList()) {
                                    sampleNames.add(sample.toString());
                                }
                                sampleString = StringUtils.join(sampleNames, ",");
                                sampleMap.put(NodeBuilder.SAMPLE, sampleString);
                            }
                            break;
                        case NodeBuilder.GENOTYPE:
                            if (!record.get(NodeBuilder.GENOTYPE).isNull()) {
                                for (Object gt : record.get(NodeBuilder.GENOTYPE).asList()) {
                                    sampleGT.add(Collections.singletonList(gt.toString()));
                                }
                            }
                            break;

                        default:
                            if (!attr.equals("core")) {
                                String[] split = attr.split("_");
                                String mainKey = attr;
                                String subKey = attr;
                                if (split.length == 2) {
                                    mainKey = split[0];
                                    subKey = split[1];
                                }
                                if (!additionalAttributes.containsKey(mainKey)) {
                                    additionalAttributes.put(mainKey, new AdditionalAttribute());
                                    additionalAttributes.get(mainKey).setAttribute(new HashMap<>());
                                }

                                additionalAttributes.get(mainKey).getAttribute().put(subKey, record.get(attr).asString());
                            }
                            break;
                    }
                }
            }

            // Set additional attributes and return variant
            if (MapUtils.isNotEmpty(sampleMap)) {
                AdditionalAttribute samplesAttribute = new AdditionalAttribute();
                samplesAttribute.setAttribute(sampleMap);
                additionalAttributes.put("samples", samplesAttribute);

            }
            if (CollectionUtils.isNotEmpty(sampleGT)) {
                List<StudyEntry> studies = new ArrayList<>();
                StudyEntry studyEntry = new StudyEntry();
                studyEntry.setFormat(Collections.singletonList("GT")).setSamplesData(sampleGT);
                studies.add(studyEntry);
                variant.setStudies(studies);
            }
            variant.getAnnotation().setAdditionalAttributes(additionalAttributes);
            return variant;
        } catch (IOException e) {
            return null;
            //throw new BioNetDBException("Executing variant query", e);
        }
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private String fixString(String in) {
        return "\"" + in + "\"";
    }
}
