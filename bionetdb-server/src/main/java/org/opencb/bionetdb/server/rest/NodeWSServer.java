package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.ApiOperation;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.server.exception.DatabaseException;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryResult;

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
@Path("/{version}/node")
@Produces("application/json")
public class NodeWSServer extends GenericRestWSServer {

    public NodeWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                        @Context HttpServletRequest hsr) throws VersionException, DatabaseException {
        super(version, uriInfo, hsr);
    }

    @GET
    @Path("/{type}/cc")
    @ApiOperation(httpMethod = "GET", value = "Get the object data model")
    public Response getModel(@PathParam("type") String type) {
        try {
             NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration);
//            Query query = new Query("id", physicalEntity);
//            query.put("nodeLabel", queryCommandOptions.nodeType);
            QueryResult queryResult = null; //networkDBAdaptor.clusteringCoefficient(new Query("id", physicalEntity));
            networkDBAdaptor.close();
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
