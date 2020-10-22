package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang.StringUtils;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.iterators.RowIterator;
import org.opencb.bionetdb.lib.db.Neo4JNetworkDBAdaptor;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 06/10/15.
 */
@Path("/{apiVersion}/table")
@Produces("application/json")
@Api(value = "Table", position = 1, description = "Methods for working with node attributes")
public class TableWSServer extends GenericRestWSServer {

    public TableWSServer(@Context UriInfo uriInfo,
                         @Context HttpServletRequest hsr) throws VersionException {
        super(uriInfo, hsr);
    }

    @GET
    @Path("/cypher")
    @ApiOperation(httpMethod = "GET", value = "Get node attribute by Cypher statement")
    public Response getNodesByCypher(@QueryParam("cypher") String cypher) {
        try {
            logger.info(cypher);

            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(bioNetDBConfiguration);
            RowIterator rowIterator = networkDBAdaptor.rowIterator(cypher);
            List<List<Object>> rows = new ArrayList<>();
            while (rowIterator.hasNext()) {
                List<Object> row = rowIterator.next();
                logger.info(StringUtils.join(row, ","));
                rows.add(row);
            }
            QueryResult<List<Object>> queryResult = new QueryResult<>(null, 0, rows.size(), rows.size(), null, null, rows);
            networkDBAdaptor.close();
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
