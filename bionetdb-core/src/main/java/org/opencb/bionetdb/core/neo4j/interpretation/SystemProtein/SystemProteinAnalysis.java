package org.opencb.bionetdb.core.neo4j.interpretation.SystemProtein;

import com.mongodb.connection.QueryResult;
import com.nimbusds.oauth2.sdk.util.MapUtils;
import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.commons.datastore.core.Query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemProteinAnalysis {

    NetworkDBAdaptor networkDBAdaptor;

    public SystemProteinAnalysis(NetworkDBAdaptor networkDBAdaptor) {
        this.networkDBAdaptor = networkDBAdaptor;
    }

    public List<Variant> execute (Pedigree pedigree, Disorder disorder, ClinicalProperty.ModeOfInheritance moi, Query query) {
        // Check moi
        Map<String, List<String>> genotypes;
        switch (moi) {
            case MONOALLELIC:
                genotypes = ModeOfInheritance.dominant(pedigree, disorder, false);
                break;
            case BIALLELIC:
                genotypes = ModeOfInheritance.recessive(pedigree, disorder, false);
                break;
            case XLINKED_MONOALLELIC:
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, true);
                break;
            case XLINKED_BIALLELIC:
                genotypes = ModeOfInheritance.xLinked(pedigree, disorder, false);
                break;
            case YLINKED:
                genotypes = ModeOfInheritance.yLinked(pedigree, disorder);
                break;
            default:
                genotypes = new HashMap<>();
                genotypes.put(pedigree.getProband().getId(), Collections.singletonList("NON_REF"));
                break;
        }
        // yLinked or other mistakes can return empty genotype lists. The next exception aims to avoid those errors.
        genotypes.entrySet().removeIf((entry) -> CollectionUtils.isEmpty(entry.getValue()));
        if (genotypes.size() == 0) {
            throw new IllegalArgumentException("Number of individuals with filled genotypes list is zero");
        }

        // Create cypher statement from query
        String cypher = "";

        // Execute cypher query

        // Convert Neo4J result into variants with system protein annotation

        // Return variants
        return null;
    }

}
