package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.Node;
import org.opencb.bionetdb.core.models.network.Relation;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
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
import java.util.ArrayList;
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
    public Response getNodes(@ApiParam(value = "Comma-separated list of node UIDs.") @QueryParam("uid") String uid,
                             @ApiParam(value = "Comma-separated list of node IDs. E.g.: ENSG00000279457") @QueryParam("id") String id,
                             @ApiParam(value = "Comma-separated list of node names. E.g.: AL627309.4,WASH7P") @QueryParam("name")
                                     String name,
                             @ApiParam(value = "Comma-separated list of node labels. E.g.: GENE,DRUG") @QueryParam("label") String label,
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

    @GET
    @Path("/stats")
    @ApiOperation(httpMethod = "GET", value = "Nodes stats")
    public Response stats(@ApiParam(value = "Comma-separated list of node attributes. E.g.: start=11869,biotype=unprocessed_pseudogene")
                          @QueryParam("attribute") String attribute
    ) {
        try {
            Query query = new Query();

            if (StringUtils.isNotEmpty(attribute)) {
                query.put("attribute", Arrays.asList(attribute.split(",")));
            }

            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            DataResult result = bioNetDbManager.getNodeQueryExecutor().stats(query);

            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/add")
    @ApiOperation(httpMethod = "GET", value = "Add node")
    public Response add(@ApiParam(value = "Node ID. E.g.: ENSG00000279457") @QueryParam("id") String id,
                        @ApiParam(value = "Node name. E.g.: AL627309.4") @QueryParam("name") String name,
                        @ApiParam(value = "Comma-separated list of node labels. E.g.: GENE,DRUG") @QueryParam("label") String label,
                        @ApiParam(value = "Comma-separated list of node attributes. E.g.: start=11869,biotype=unprocessed_pseudogene")
                        @QueryParam("attribute") String attribute
    ) {
        try {
            // Create node
            Node node = buildNode(id, name, label, attribute);

            // Add node to the BioNetDB
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            long uid = bioNetDbManager.getNodeQueryExecutor().add(node);

            return createOkResponse("Added. UID node: " + uid);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/update")
    @ApiOperation(httpMethod = "GET", value = "Update node")
    public Response update(@ApiParam(value = "Node ID. E.g.: ENSG00000279457") @QueryParam("id") String id,
                           @ApiParam(value = "Node name. E.g.: AL627309.4") @QueryParam("name") String name,
                           @ApiParam(value = "Comma-separated list of node labels. E.g.: GENE,DRUG") @QueryParam("label") String label,
                           @ApiParam(value = "Comma-separated list of node attributes. E.g.: start=11869,biotype=unprocessed_pseudogene")
                           @QueryParam("attribute") String attribute
    ) {
        try {
            // Create node
            Node node = buildNode(id, name, label, attribute);

            // Update node in the BioNetDB
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            bioNetDbManager.getNodeQueryExecutor().update(node);

            return createOkResponse("Updated.");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/delete")
    @ApiOperation(httpMethod = "GET", value = "Delete node")
    public Response delete(@ApiParam(value = "Node ID. E.g.: ENSG00000279457") @QueryParam("id") String id,
                           @ApiParam(value = "Comma-separated list of node labels. E.g.: GENE,DRUG") @QueryParam("label") String label
    ) {
        try {
            // Create node
            Node node = buildNode(id, null, label, null);

            // Delete node from the BioNetDB
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            bioNetDbManager.getNodeQueryExecutor().delete(node);

            return createOkResponse("Deleted.");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/link")
    @ApiOperation(httpMethod = "GET", value = "Link origin node to destination node")
    public Response link(@ApiParam(value = "Origin node ID. E.g.: ENSG00000279457") @QueryParam("origId") String origId,
                         @ApiParam(value = "Origin node label. E.g.: GENE") @QueryParam("origLabel") String origLabel,
                         @ApiParam(value = "Destination node ID. E.g.: ENST00000023435") @QueryParam("destId") String destId,
                         @ApiParam(value = "Destination node label. E.g.: TRANSCRIPT") @QueryParam("destLabel") String destLabel,
                         @ApiParam(value = "Relation label. E.g.: HAS") @QueryParam("relationLabel") String relationType,
                         @ApiParam(value = "Comma-separated list of relation attributes. E.g.: time=11869") @QueryParam("attribute") String
                                     relationAttr
    ) {
        try {
            // Create node
            Node origNode = buildNode(origId, null, origLabel, null);
            Node destNode = buildNode(destId, null, destLabel, null);

            Relation relation = buildRelation(relationType, relationAttr);

            // Link origin and destination nodes
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            bioNetDbManager.getNodeQueryExecutor().link(origNode, destNode, relation);

            return createOkResponse("Linked.");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/updateLink")
    @ApiOperation(httpMethod = "GET", value = "Update link")
    public Response updateLink(@ApiParam(value = "Origin node ID. E.g.: ENSG00000279457") @QueryParam("origId") String origId,
                               @ApiParam(value = "Origin node label. E.g.: GENE") @QueryParam("origLabel") String origLabel,
                               @ApiParam(value = "Destination node ID. E.g.: ENST00000023435") @QueryParam("destId") String destId,
                               @ApiParam(value = "Destination node label. E.g.: TRANSCRIPT") @QueryParam("destLabel") String destLabel,
                               @ApiParam(value = "Relation label. E.g.: HAS") @QueryParam("relationLabel") String relationLabel,
                               @ApiParam(value = "Comma-separated list of relation attributes. E.g.: time=11869") @QueryParam("attribute")
                                           String relationAttr
    ) {
        try {
            // Create node
            Node origNode = buildNode(origId, null, origLabel, null);
            Node destNode = buildNode(destId, null, destLabel, null);

            Relation relation = buildRelation(relationLabel, relationAttr);

            // Link origin and destination nodes
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            bioNetDbManager.getNodeQueryExecutor().updateLink(origNode, destNode, relation);

            return createOkResponse("Updated.");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/unlink")
    @ApiOperation(httpMethod = "GET", value = "Delete link")
    public Response unlink(@ApiParam(value = "Origin node ID. E.g.: ENSG00000279457") @QueryParam("origId") String origId,
                           @ApiParam(value = "Origin node label. E.g.: GENE") @QueryParam("origLabel") String origLabel,
                           @ApiParam(value = "Destination node ID. E.g.: ENST00000023435") @QueryParam("destId") String destId,
                           @ApiParam(value = "Destination node label. E.g.: TRANSCRIPT") @QueryParam("destLabel") String destLabel,
                           @ApiParam(value = "Relation label. E.g.: HAS") @QueryParam("relationLabel") String relationLabel
    ) {
        try {
            // Create origin and destionation nodes
            Node origNode = buildNode(origId, null, origLabel, null);
            Node destNode = buildNode(destId, null, destLabel, null);

            // Create relation
            Relation relation = buildRelation(relationLabel, null);

            // Delete origin-destination link
            BioNetDbManager bioNetDbManager = new BioNetDbManager(bioNetDBConfiguration);
            bioNetDbManager.getNodeQueryExecutor().unlink(origNode, destNode, relation);

            return createOkResponse("Unliked.");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private Node buildNode(String id, String name, String label, String attribute) throws BioNetDBException {
        if (StringUtils.isEmpty(id)) {
            throw new BioNetDBException("Missing node ID");
        }

        if (StringUtils.isEmpty(label)) {
            throw new BioNetDBException("Missing node label");
        }

        List<String> tags = Arrays.asList(label.split(","));
        List<Node.Label> labels = new ArrayList<>();
        for (String tag : tags) {
            labels.add(Node.Label.valueOf(tag));
        }

        // Create node
        Node node = new Node()
                .setId(id)
                .setName(name)
                .setLabels(labels);

        // Parse attributes
        ObjectMap attrs = new ObjectMap();
        if (StringUtils.isNotEmpty(attribute)) {
            for (String entry : Arrays.asList(attribute.split(","))) {
                String[] split = entry.split("=");
                attrs.put(split[0], split[1]);
            }
        }
        node.setAttributes(attrs);

        return node;
    }

    //-------------------------------------------------------------------------

    private Relation buildRelation(String label, String attribute) throws BioNetDBException {
        if (StringUtils.isEmpty(label)) {
            throw new BioNetDBException("Missing relation label");
        }

        // Create relation
        Relation relation = new Relation()
                .setLabel(Relation.Label.valueOf(label));

        // Parse attributes
        ObjectMap attrs = new ObjectMap();
        if (StringUtils.isNotEmpty(attribute)) {
            for (String entry : Arrays.asList(attribute.split(","))) {
                String[] split = entry.split("=");
                attrs.put(split[0], split[1]);
            }
        }
        relation.setAttributes(attrs);

        return relation;
    }
}
