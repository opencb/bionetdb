package org.opencb.bionetdb.core.utils.converters;

import org.neo4j.driver.v1.Record;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.Converter;
import org.opencb.bionetdb.core.utils.NodeBuilder;

public class RecordToVariantConverter implements Converter<Record, Variant> {

    @Override
    public Variant convert(Record record) {
        VariantBuilder variantBuilder = Variant.newBuilder();
        variantBuilder.setChromosome(record.get(NodeBuilder.CHROMOSOME).asString());
        variantBuilder.setStart(Integer.parseInt(record.get(NodeBuilder.START).asString()));
        variantBuilder.setEnd(Integer.parseInt(record.get(NodeBuilder.END).asString()));
        variantBuilder.setReference(record.get(NodeBuilder.REFERENCE).asString());
        variantBuilder.setAlternate(record.get(NodeBuilder.ALTERNATE).asString());
        variantBuilder.setType(VariantType.valueOf(record.get(NodeBuilder.TYPE).asString()));
        variantBuilder.setStudyId("S");
        variantBuilder.setFormat("GT");
        for (int i = 0; i < record.get("num_of_sam").asInt(); i++) {
            variantBuilder.addSample(record.get("sam_collection").get(i).asString(), record.get("gt_collection").get(i).asString());
        }
        return variantBuilder.build();
    }
}
