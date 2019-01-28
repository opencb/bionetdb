package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.VariantsPair;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Arrays;
import java.util.List;

@Path("/{apiVersion}/analysis")
@Produces("application/json")
@Api(value = "Analysis", position = 1, description = "Methods for working with 'nodes'")
public class AnalysisWSServer extends GenericRestWSServer {

    public AnalysisWSServer(@Context UriInfo uriInfo,
                            @Context HttpServletRequest hsr) throws VersionException {
        super(uriInfo, hsr);
    }

    @GET
    @Path("/matchingDominantVariants")
    @ApiOperation(httpMethod = "GET", value = "Get dominant variants")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query")
    })
    public Response getMatchingVariants() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<Variant> variants = bioNetDbManager.getMatchingDominantVariants("NA12877", "NA12879", "NA12878", queryOptions);
            return createOkResponse(new QueryResult<>("id", 0, variants.size(), variants.size(), "", "", variants));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/matchingRecessiveVariants")
    @ApiOperation(httpMethod = "GET", value = "Get recessive variants")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query")
    })
    public Response getMatchingRecessiveVariants() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<Variant> variants = bioNetDbManager.getMatchingRecessiveVariants("NA12877", "NA12879", "NA12878", queryOptions);
            return createOkResponse(new QueryResult<>("id", 0, variants.size(), variants.size(), "", "", variants));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/matchingDeNovoVariants")
    @ApiOperation(httpMethod = "GET", value = "Get de novo variants")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query")
    })
    public Response getMatchingDeNovoVariants() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<Variant> variants = bioNetDbManager.getMatchingDeNovoVariants("NA12877", "NA12879", "NA12878", queryOptions);
            return createOkResponse(new QueryResult<>("id", 0, variants.size(), variants.size(), "", "", variants));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/matchingXLinkedVariants")
    @ApiOperation(httpMethod = "GET", value = "Get X linked variants")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query")
    })
    public Response getMatchingXLinkedVariants() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<Variant> variants = bioNetDbManager.getMatchingXLinkedVariants("NA12877", "NA12879", "NA12878", queryOptions);
            return createOkResponse(new QueryResult<>("id", 0, variants.size(), variants.size(), "", "", variants));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/matchingVariantsInSameGen")
    @ApiOperation(httpMethod = "GET", value = "Get compounded heterozygote variants")
    public Response getMatchingVariantsInSameGen() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<VariantsPair> listVaraintsPair = bioNetDbManager.getMatchingVariantsInSameGen("NA12877", "NA12879", "NA12878", 5);
            return createOkResponse(new QueryResult<>("id", 0, listVaraintsPair.size(), listVaraintsPair.size(), "", "", listVaraintsPair));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/specificBurdenTest")
    @ApiOperation(httpMethod = "GET", value = "Get Burden test")
    public Response getSpecificBurdenTest() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<String> resultBurdenTest = bioNetDbManager.getSpecificBurdenTest(Arrays.asList("PMS2-001", "AP001482.1-201"));
            return createOkResponse(new QueryResult<>("id", 0, resultBurdenTest.size(), resultBurdenTest.size(), "", "", resultBurdenTest));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
