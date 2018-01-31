package org.opencb.bionetdb.server.ws;

import io.swagger.annotations.ApiOperation;
import org.opencb.bionetdb.core.network.Network;
import org.opencb.bionetdb.server.exception.DatabaseException;
import org.opencb.bionetdb.server.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Created by imedina on 06/10/15.
 */
@Path("/{version}/{database}/network")
@Produces("application/json")
public class NetworkWSServer extends GenericRestWSServer {

    public NetworkWSServer(@PathParam("version") String version, @PathParam("database") String database,
                           @Context UriInfo uriInfo, @Context HttpServletRequest hsr) throws VersionException, DatabaseException {
        super(version, database, uriInfo, hsr);
    }

    @GET
    @Path("/model")
    @ApiOperation(httpMethod = "GET", value = "Get the object data model")
    public Response getModel() {
        return createModelResponse(Network.class);
    }
}
