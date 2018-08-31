package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/{apiVersion}/analysis")
@Produces("application/json")
@Api(value = "Analysis", position = 1, description = "Methods for working with 'nodes'")
public class AnalysisWSServer extends GenericRestWSServer {

    public AnalysisWSServer(@Context UriInfo uriInfo,
                        @Context HttpServletRequest hsr) throws VersionException {
        super(uriInfo, hsr);
    }

    @GET
    @Path("/variants")
    @ApiOperation(httpMethod = "GET", value = "Get Nodes by ID")
    public Response getNodesById() {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            QueryResult<String> helloWorld = bioNetDbManager.getDominantVariants();
            return createOkResponse(helloWorld);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
