package org.opencb.bionetdb.lib.db.converters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Record;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.bionetdb.core.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            // Create variant and prepare additional attributes
            Variant variant = Utils.uncompress(fixString(record.get("attr_core").asString()), Variant.class, objMapper);
            Map<String, AdditionalAttribute> additionalAttributes = new HashMap<>();

            // For sample attributes
            String sampleNames = null;
            String sampleGenotypes = null;

            for (String attr : record.keys()) {
                if (!record.get(attr).isNull() && !"null".equals(record.get(attr).toString())) {
                    switch (attr) {
                        case "attr_studies":
                            List<org.opencb.biodata.models.variant.StudyEntry> studies =
                                    Utils.uncompressList(fixString(record.get(attr).asString()), StudyEntry.class, objMapper);
                            variant.setStudies(studies);
                            break;
                        case "attr_consequenceTypes":
                            List<ConsequenceType> ct = Utils.uncompressList(fixString(record.get(attr).asString()), ConsequenceType.class,
                                    objMapper);
                            variant.getAnnotation().setConsequenceTypes(ct);
                            break;
                        case "attr_xrefs":
                            List<Xref> xrefs = Utils.uncompressList(fixString(record.get(attr).asString()), Xref.class, objMapper);
                            variant.getAnnotation().setXrefs(xrefs);
                            break;
                        case "attr_populationFrequencies":
                            List<PopulationFrequency> popFreqs = Utils.uncompressList(fixString(record.get(attr).asString()),
                                    PopulationFrequency.class, objMapper);
                            variant.getAnnotation().setPopulationFrequencies(popFreqs);
                            break;
                        case "attr_conservation":
                            List<Score> conservation = Utils.uncompressList(fixString(record.get(attr).asString()), Score.class, objMapper);
                            variant.getAnnotation().setConservation(conservation);
                            break;
                        case "attr_geneExpression":
                            List<Expression> expression = Utils.uncompressList(fixString(record.get(attr).asString()), Expression.class,
                                    objMapper);
                            variant.getAnnotation().setGeneExpression(expression);
                            break;
                        case "attr_geneTraitAssociation":
                            List<GeneTraitAssociation> gta = Utils.uncompressList(fixString(record.get(attr).asString()),
                                    GeneTraitAssociation.class, objMapper);
                            variant.getAnnotation().setGeneTraitAssociation(gta);
                            break;
                        case "attr_geneDrugInteraction":
                            List<GeneDrugInteraction> gdi = Utils.uncompressList(fixString(record.get(attr).asString()),
                                    GeneDrugInteraction.class, objMapper);
                            variant.getAnnotation().setGeneDrugInteraction(gdi);
                            break;
                        case "attr_traitAssociation":
                            List<EvidenceEntry> ta = Utils.uncompressList(fixString(record.get(attr).asString()), EvidenceEntry.class,
                                    objMapper);
                            variant.getAnnotation().setTraitAssociation(ta);
                            break;
                        case "attr_functionalScore":
                            List<Score> fs = Utils.uncompressList(fixString(record.get(attr).asString()), Score.class, objMapper);
                            variant.getAnnotation().setFunctionalScore(fs);
                            break;
                        case NodeBuilder.SAMPLE:
                            sampleNames = StringUtils.join(record.get(NodeBuilder.SAMPLE).asList(), ",");
                            break;
                        case NodeBuilder.GENOTYPE:
                            sampleGenotypes = StringUtils.join(record.get(NodeBuilder.GENOTYPE).asList(), ",");
                            break;

                        default:
                            if (!attr.equals("attr_core")) {
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
            if (StringUtils.isNotEmpty(sampleNames) && StringUtils.isNotEmpty(sampleGenotypes)) {
                AdditionalAttribute sampleAttrs = new AdditionalAttribute();
                Map<String, String> map = new HashMap<>();
                map.put(NodeBuilder.SAMPLE, sampleNames);
                map.put(NodeBuilder.GENOTYPE, sampleGenotypes);
                sampleAttrs.setAttribute(map);
                additionalAttributes.put("samples", sampleAttrs);

//                // And set sample data
//                StudyEntry studyEntry = new StudyEntry();
//                studyEntry.setFormat(Collections.singletonList("GT"));
//                List<List<String>> sampleData = new ArrayList<>();
//                for (String gt : sampleGenotypes.split(",")) {
//                    sampleData.add(Collections.singletonList(gt));
//                }
//                studyEntry.setSamplesData(sampleData);
//                variant.setStudies(Collections.singletonList(studyEntry));
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
