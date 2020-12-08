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
    public Response getNodes(@ApiParam(value = "Comma-separated list of node UIDs.") @QueryParam("uid") String uid,
                             @ApiParam(value = "Comma-separated list of node IDs. E.g.: ENSG00000279457") @QueryParam("id") String id,
                             @ApiParam(value = "Comma-separated list of node names. E.g.: AL627309.4,WASH7P") @QueryParam("name")
                                         String name,
                             @ApiParam(value = "Comma-separated list of node labels. E.g.: GENE,DRUG") @QueryParam("label") String label,
                             @ApiParam(value = "Comma-separated list of node sources. E.g.: ensembl") @QueryParam("source") String source,
                             @ApiParam(value = "Comma-separated list of node attributes. E.g.: start=11869,biotype=unprocessed_pseudogene")
                                 @QueryParam("attribute") String attribute,
                             @ApiParam(value = "Number of nodes to return.", defaultValue = "25") @QueryParam(QueryOptions.LIMIT) int limit
    ) {
        try {
            Query query = new Query();
            if (StringUtils.isNotEmpty(uid)) {
                query.put("uid", Arrays.asList(uid.split(",")));
            }

            if (StringUtils.isNotEmpty(id)) {
                query.put("id", Arrays.asList(id.split(",")));
            }

            if (StringUtils.isNotEmpty(name)) {
                query.put("name", Arrays.asList(name.split(",")));
            }

            if (StringUtils.isNotEmpty(label)) {
                query.put("label", Arrays.asList(label.split(",")));
            }

            if (StringUtils.isNotEmpty(source)) {
                query.put("source", Arrays.asList(source.split(",")));
            }

            if (StringUtils.isNotEmpty(attribute)) {
                query.put("attribute", Arrays.asList(attribute.split(",")));
            }

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.put(QueryOptions.LIMIT, limit);

            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            DataResult result = bioNetDbManager.getNodeQueryExecutor().query(query, queryOptions);

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
