package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.bionetdb.server.exception.VersionException;
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
    @Path("/query")
    @ApiOperation(httpMethod = "GET", value = "Query nodes")
    public Response getNetworkPaths(@ApiParam(value = "Origin node label. E.g.: GENE") @QueryParam("origLabel") String origLabel,
                                    @ApiParam(value = "Comma-separated list of origin node filters. E.g.: start=11869,biotype=unprocessed_"
                                            + "pseudogene") @QueryParam("origFilter") String origFilter,
                                    @ApiParam(value = "Destination node label.") @QueryParam("destLabel") String destLabel,
                                    @ApiParam(value = "Comma-separated list of destination node filters.") @QueryParam("destFilter")
                                            String destFilter,
                                    @ApiParam(value = "Maximum number of hops.", defaultValue = "3") @QueryParam("maxNumHops")
                                            int maxNumHops,
                                    @ApiParam(value = "Number of network paths to return.",
                                            defaultValue = "25") @QueryParam(QueryOptions.LIMIT) int limit
    ) {
        try {
            Query query = new Query();

            query.put("origin_label", origLabel);

            if (StringUtils.isNotEmpty(origFilter)) {
                query.put("origin_filters", Arrays.asList(origFilter.split(",")));
            }

            query.put("destination_label", destLabel);

            if (StringUtils.isNotEmpty(destFilter)) {
                query.put("destination_filters", Arrays.asList(destFilter.split(",")));
            }

            query.put("max_num_Hops", maxNumHops);

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.put(QueryOptions.LIMIT, limit);

            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            BioNetDBResult<NetworkPath> result = bioNetDbManager.getPathQueryExecutor().query(query, queryOptions);

            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/cypher")
    @ApiOperation(httpMethod = "GET", value = "Get Nodes by Cypher statement")
    public Response getNetworkPathsByCypher(@QueryParam("cypher") String cypher) {
        try {
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            BioNetDBResult<NetworkPath> result = bioNetDbManager.getPathQueryExecutor().query(cypher);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @GET
//    @Path("/cypher")
//    @ApiOperation(httpMethod = "GET", value = "Get network path by Cypher statement")
//    public Response getNetworkPathByCypher(@QueryParam("cypher") String cypher) {
//        try {
//            logger.info(cypher);
//
//            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(bioNetDBConfiguration);
//            List<NetworkPath> networkPaths = new ArrayList<>();
////            NetworkPathIterator iterator = networkDBAdaptor.networkPathIterator(cypher);
////            while (iterator.hasNext()) {
////                NetworkPath networkPath = iterator.next();
////                networkPaths.add(networkPath);
////            }
//            QueryResult<NetworkPath> queryResult = new QueryResult<>(null, 0, networkPaths.size(), networkPaths.size(),
//                    null, null, networkPaths);
//            networkDBAdaptor.close();
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
}
