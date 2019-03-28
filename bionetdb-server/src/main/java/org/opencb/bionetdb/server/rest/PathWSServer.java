package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.api.iterators.NetworkPathIterator;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jtarraga on 13/03/18.
 */
@Path("/{apiVersion}/path")
@Produces("application/json")
@Api(value = "Path", position = 1, description = "Methods for working with network paths")
public class PathWSServer extends GenericRestWSServer {

    public PathWSServer(@Context UriInfo uriInfo,
                        @Context HttpServletRequest hsr) throws VersionException {
        super(uriInfo, hsr);
    }

    @GET
    @Path("/cypher")
    @ApiOperation(httpMethod = "GET", value = "Get network path by Cypher statement")
    public Response getNetworkPathByCypher(@QueryParam("cypher") String cypher) {
        try {
            logger.info(cypher);

            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration);
            NetworkPathIterator iterator = networkDBAdaptor.networkPathIterator(cypher);
            List<NetworkPath> networkPaths = new ArrayList<>();
            while (iterator.hasNext()) {
                NetworkPath networkPath = iterator.next();
                networkPaths.add(networkPath);
            }
            QueryResult<NetworkPath> queryResult = new QueryResult<>(null, 0, networkPaths.size(), networkPaths.size(),
                    null, null, networkPaths);
            networkDBAdaptor.close();
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
