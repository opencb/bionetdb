package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 06/10/15.
 */
@Path("/{apiVersion}/node")
@Produces("application/json")
@Api(value = "Node", position = 1, description = "Methods for working with 'nodes'")
public class NodeWSServer extends GenericRestWSServer {

    public NodeWSServer(@Context UriInfo uriInfo,
                        @Context HttpServletRequest hsr) throws VersionException {
        super(uriInfo, hsr);
    }

    @GET
    @Path("/query")
    @ApiOperation(httpMethod = "GET", value = "Query nodes")
    public Response getNodes(@ApiParam(value = "Comma-separated list of IDs") @QueryParam("id") String id,
                             @ApiParam(value = "Comma-separated list of node labels") @QueryParam("label") String label
    ) {
        try {
            Query query = new Query();
            if (StringUtils.isNotEmpty(id)) {
                List<String> ids = Arrays.asList(id.split(","));
                query.put("id", ids);
            }

            if (StringUtils.isNotEmpty(label)) {
                List<String> labels = Arrays.asList(id.split(","));
                query.put("label", labels);
            }

            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            DataResult result = bioNetDbManager.getNodeQueryExecutor().query(query, QueryOptions.empty());

            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @GET
//    @Path("/{id}/info")
//    @ApiOperation(httpMethod = "GET", value = "Get Nodes by ID")
//    public Response getNodesById(@PathParam("id") String type) {
//        try {
//             NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(database, bioNetDBConfiguration);
////            Query query = new Query("id", physicalEntity);
////            query.put("nodeLabel", queryCommandOptions.nodeType);
//            QueryResult queryResult = null; //networkDBAdaptor.clusteringCoefficient(new Query("id", physicalEntity));
//            networkDBAdaptor.close();
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/cypher")
    @ApiOperation(httpMethod = "GET", value = "Get Nodes by Cypher statement")
    public Response getNodesByCypher(@QueryParam("cypher") String cypher) {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            BioNetDBResult<Node> result = bioNetDbManager.getNodeQueryExecutor().query(cypher);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
