package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.neo4j.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.core.network.Node;
import org.opencb.bionetdb.server.exception.DatabaseException;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.List;

/**
 * Created by imedina on 06/10/15.
 */
@Path("/{version}/node")
@Produces("application/json")
@Api(value = "Node", position = 1, description = "Methods for working with 'nodes'")
public class NodeWSServer extends GenericRestWSServer {

    public NodeWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                        @Context HttpServletRequest hsr) throws VersionException, DatabaseException {
        super(version, uriInfo, hsr);
    }

    @GET
    @Path("/{id}/info")
    @ApiOperation(httpMethod = "GET", value = "Get Nodes by ID")
    public Response getNodesById(@PathParam("id") String type) {
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

    @GET
    @Path("/cypher")
    @ApiOperation(httpMethod = "GET", value = "Get Nodes by Cypher statement")
    public Response getNodesByCypher(@QueryParam("cypher") String cypher) {
        try {
            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration);
            List<Node> nodes = networkDBAdaptor.nodeQuery(cypher);
            logger.info(cypher);
            for (Node node: nodes) {
                logger.info(node.toStringEx());
            }
            QueryResult<Node> queryResult = new QueryResult<>(null, 0, nodes.size(), nodes.size(), null, null, nodes);
            networkDBAdaptor.close();
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
