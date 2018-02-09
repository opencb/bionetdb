package org.opencb.bionetdb.core.api.query;

import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

public class GeneNodeQueryParam extends NodeQueryParam {

    public static final String REGION_DESCR = "List of regions: {chr}:{start}-{end}, e.g.: 2,3:1000000-2000000";
    public static final GeneNodeQueryParam REGION = new GeneNodeQueryParam("region", TEXT_ARRAY, REGION_DESCR);

    public static final String TRAIT_DESCR = "List of trait association names (or IDs): e.g.: \"Cardiovascular Diseases, OMIM:269600\"";
    public static final GeneNodeQueryParam TRAIT = new GeneNodeQueryParam("trait", TEXT_ARRAY, TRAIT_DESCR);

    public static final String DRUG_DESCR = "List of drug names";
    public static final GeneNodeQueryParam DRUG = new GeneNodeQueryParam("drug", TEXT_ARRAY, DRUG_DESCR);

    private GeneNodeQueryParam(String key, Type type, String description) {
        super(key, type, description);
    }
}

