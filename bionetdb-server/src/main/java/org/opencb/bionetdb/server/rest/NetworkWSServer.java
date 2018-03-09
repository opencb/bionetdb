package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.*;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.server.exception.DatabaseException;
import org.opencb.bionetdb.server.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Created by imedina on 06/10/15.
 */
@Path("/{version}/network")
@Produces("application/json")
@Api(value = "Network", position = 7, description = "Methods for working with 'samples' endpoint")
public class NetworkWSServer extends GenericRestWSServer {

    public NetworkWSServer(@PathParam("version") String version,  @Context UriInfo uriInfo,
                           @Context HttpServletRequest hsr) throws VersionException, DatabaseException {
        super(version, uriInfo, hsr);
    }

    @GET
    @Path("/model")
    @ApiOperation(httpMethod = "GET", value = "Get the object data model")
    public Response getModel() {
        return createModelResponse(Network.class);
    }

    @GET
    @Path("/info")
    @ApiOperation(value = "Get sample information", position = 1, response = Network.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeIndividual", value = "Include Individual object as an attribute",
                    defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Sample apiVersion") @QueryParam("version") Integer version,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false")
            @QueryParam("silent") boolean silent) {
        try {
//            List<String> sampleList = getIdList(samplesStr);
//            List<QueryResult<Sample>> sampleQueryResult = sampleManager.get(studyStr, sampleList, query, queryOptions, silent, sessionId);
            return createOkResponse("hello!");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
