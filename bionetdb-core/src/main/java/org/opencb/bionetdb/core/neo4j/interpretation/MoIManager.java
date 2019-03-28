package org.opencb.bionetdb.core.neo4j.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.types.Node;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.RowIterator;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.query.Neo4JQueryParser;
import org.opencb.bionetdb.core.neo4j.query.Neo4JVariantQueryParam;
import org.opencb.bionetdb.core.utils.Neo4jConverter;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.*;

public class MoIManager {

    private NetworkDBAdaptor networkDBAdaptor;

    public MoIManager(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public List<Variant> getDominantVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.dominant(pedigree, disorder, false);
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.recessive(pedigree, disorder, false);
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getXLinkedDominantVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true);
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getXLinkedRecessiveVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false);
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "X");
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getYLinkedVariants(Pedigree pedigree, Disorder disorder, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.yLinked(pedigree, disorder);
        query.put(Neo4JVariantQueryParam.CHROMOSOME.key(), "Y");
        putGenotypes(query, genotypes);

        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());
        return queryVariants(cypher);
    }

    public List<Variant> getDeNovoVariants(Pedigree pedigree, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.deNovo(pedigree);
        putGenotypes(query, genotypes);

        query.put(Neo4JVariantQueryParam.INCLUDE_STUDY.key(), true);
        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());

        // Get variants and genotypes
        List<Variant> variants = queryVariantGenotypes(cypher);
        if (CollectionUtils.isEmpty(variants)) {
            return Collections.emptyList();
        } else {
            List<String> sampleNames = Arrays.asList(variants.get(0).getStudies().get(0).getFiles().get(0).getAttributes()
                    .get("sampleNames")
                    .split(","));

            // Filter deNovo variants
            int probandSampleIdx = sampleNames.indexOf(pedigree.getProband().getId());
            int motherSampleIdx = sampleNames.indexOf(pedigree.getProband().getMother().getId());
            int fatherSampleIdx = sampleNames.indexOf(pedigree.getProband().getFather().getId());
            return ModeOfInheritance.deNovo(variants.iterator(), probandSampleIdx, motherSampleIdx, fatherSampleIdx);
        }
    }

    public Map<String, List<Variant>> getCompoundHeterozygousVariants(Pedigree pedigree, Query query) throws BioNetDBException {
        Map<String, List<String>> genotypes = ModeOfInheritance.compoundHeterozygous(pedigree);
        putGenotypes(query, genotypes);

        query.put(Neo4JVariantQueryParam.INCLUDE_STUDY.key(), true);
        String cypher = Neo4JQueryParser.parseVariantQuery(query, QueryOptions.empty());

        // Get variants and genotypes
        List<Variant> variants = queryVariantGenotypes(cypher);
        if (CollectionUtils.isEmpty(variants)) {
            return Collections.emptyMap();
        } else {
            List<String> sampleNames = Arrays.asList(variants.get(0).getStudies().get(0).getFiles().get(0).getAttributes()
                    .get("sampleNames")
                    .split(","));

            // Filter compoundHeterozygous variants
            int probandSampleIdx = sampleNames.indexOf(pedigree.getProband().getId());
            int motherSampleIdx = sampleNames.indexOf(pedigree.getProband().getMother().getId());
            int fatherSampleIdx = sampleNames.indexOf(pedigree.getProband().getFather().getId());

            System.out.println("variants size = " + variants.size());
            return ModeOfInheritance.compoundHeterozygous(variants.iterator(), probandSampleIdx, motherSampleIdx, fatherSampleIdx);
        }
    }

    //------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //------------------------------------------------------------------

    private void putGenotypes(Query query, Map<String, List<String>> genotypes) {
        // genotype format: sample:0/1,1/1,....
        List<String> gt = new ArrayList<>();
        for (String sample : genotypes.keySet()) {
            gt.add(sample + ":" + StringUtils.join(genotypes.get(sample), ","));
        }
        query.put(Neo4JVariantQueryParam.GENOTYPE.key(), gt);
    }

    private List<Variant> queryVariants(String cypher) throws BioNetDBException {
        List<Variant> variants = new ArrayList<>();

        String aReturn = cypher.split("RETURN")[1];
        String[] attrs = aReturn.split(",");

        RowIterator rowIterator = networkDBAdaptor.rowIterator(cypher);
        while (rowIterator.hasNext()) {
            List<Object> next = rowIterator.next();
            // TODO: convert "next" to variant
            //variants.add(NodeBuilder.newVariant(nodeIterator.next()));
        }

        return variants;
    }


    /*
        public RowIterator rowIterator(String cypher) throws BioNetDBException {
        Session session = this.driver.session();
//        System.out.println("Cypher query: " + cypher);
        return new Neo4JRowIterator(session.run(cypher));
    }

     */

    private List<Variant> queryVariantGenotypes(String cypher) throws BioNetDBException {
        List<Variant> variants = new ArrayList<>();

        String names = null;

        RowIterator rowIterator = networkDBAdaptor.rowIterator(cypher);
        while (rowIterator.hasNext()) {
            List<Object> row = rowIterator.next();

            // Variant
            Variant variant = NodeBuilder.newVariant(Neo4jConverter.toNode((org.neo4j.driver.v1.types.Node) row.get(0)));

            // Sample
            if (names == null) {
                List<String> sampleNames = new ArrayList<>();
                List<org.neo4j.driver.v1.types.Node> sampleNodes = (List<org.neo4j.driver.v1.types.Node>) row.get(1);
                for (Node sampleNode : sampleNodes) {
                    sampleNames.add(sampleNode.get("name").asString());
                }
                names = StringUtils.join(sampleNames, ",");
            }
            variant.getStudies().get(0).setAttributes(new HashMap<>());
            variant.getStudies().get(0).getAttributes().put("sampleNames", names);

            // Genotype
            List<org.neo4j.driver.v1.types.Node> gtNodes = (List<org.neo4j.driver.v1.types.Node>) row.get(2);
            List<List<String>> sampleData = new ArrayList<>(gtNodes.size());
            for (Node gtNode : gtNodes) {
                sampleData.add(Collections.singletonList(gtNode.get("attr_GT").asString()));
            }
            variant.getStudies().get(0).setSamplesData(sampleData);

            variants.add(variant);
        }
        return variants;
    }
}
