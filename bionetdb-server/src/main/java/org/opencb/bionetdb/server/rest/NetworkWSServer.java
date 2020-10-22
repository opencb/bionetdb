package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.*;
import org.opencb.bionetdb.core.models.network.Network;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Created by imedina on 06/10/15.
 */
@Path("/{apiVersion}/network")
@Produces("application/json")
@Api(value = "Network", position = 7, description = "Methods for working with 'samples' endpoint")
public class NetworkWSServer extends GenericRestWSServer {

    public NetworkWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest hsr)
            throws VersionException {
        super(uriInfo, hsr);
    }

    @GET
    @Path("/model")
    @ApiOperation(httpMethod = "GET", value = "Get the object data model")
    public Response getModel() {
        return createModelResponse(Network.class);
    }

    @GET
    @Path("/info")
    @ApiOperation(value = "Get network information", position = 1, response = Network.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false")
            @QueryParam("silent") boolean silent) {
        try {
            return createOkResponse("Nto yet implemented");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Get network information", position = 1, response = Network.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    dataType = "string", paramType = "query")
    })
    public Response query(
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false")
            @QueryParam("silent") boolean silent) {
        try {
            return createOkResponse("Nto yet implemented");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/cypher")
    @ApiOperation(value = "Execute a cypher query in the server", position = 1, response = Network.class)
    public Response cypher(
            @ApiParam(value = "Cypher query")
            @QueryParam("query") String cypherQuery) {
        try {
            QueryResult<Network> networkQueryResult = bioNetDBManager.networkQuery(cypherQuery);
            return createOkResponse(networkQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
