package org.opencb.bionetdb.core.utils.converters;

import org.neo4j.driver.v1.Record;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.Converter;
import org.opencb.bionetdb.core.utils.NodeBuilder;

import java.util.HashMap;
import java.util.Map;

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

        Map<String, String> systemMap = new HashMap<>();
        if (!record.get(NodeBuilder.TARGET_PROTEIN).isNull()) {
            systemMap.put(NodeBuilder.TARGET_PROTEIN, record.get(NodeBuilder.TARGET_PROTEIN).asString());
        }
        if (!record.get(NodeBuilder.NEXUS).isNull()) {
            systemMap.put(NodeBuilder.NEXUS, record.get(NodeBuilder.NEXUS).asString());
        }
        if (!record.get(NodeBuilder.PANEL_PROTEIN).isNull()) {
            systemMap.put(NodeBuilder.PANEL_PROTEIN, record.get(NodeBuilder.PANEL_PROTEIN).asString());
        }
        if (!record.get(NodeBuilder.PANEL_GENE).isNull()) {
            systemMap.put(NodeBuilder.PANEL_GENE, record.get(NodeBuilder.PANEL_GENE).asString());
        }
        variantBuilder.setAttributes(systemMap);

        return variantBuilder.build();
    }
}
