package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.VariantsPair;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
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
    @Path("/matchingVariants")
    @ApiOperation(httpMethod = "GET", value = "Get dominant variants")
    public Response getMatchingVariants() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            List<Variant> variants = bioNetDbManager.getMatchingVariants();
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
            List<VariantsPair> listVaraintsPair = bioNetDbManager.getMatchingVariantsInSameGen();
            return createOkResponse(new QueryResult<>("id", 0, listVaraintsPair.size(), listVaraintsPair.size(), "", "", listVaraintsPair));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
