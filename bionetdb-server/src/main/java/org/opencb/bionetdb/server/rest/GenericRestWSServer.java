package org.opencb.bionetdb.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.google.common.base.Splitter;
import io.swagger.annotations.ApiParam;
import org.opencb.bionetdb.core.BioNetDBManager;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.config.DatabaseConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.server.exception.DatabaseException;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.util.*;

/**
 * Created by imedina on 01/10/15.
 */
@ApplicationPath("/")
@Path("/{version}")
@Produces(MediaType.APPLICATION_JSON)
public class GenericRestWSServer {

    @DefaultValue("")
    @PathParam("version")
    @ApiParam(name = "version", value = "Use 'latest' for last stable version", allowableValues = "v1", defaultValue = "v1")
    protected String version;

    protected String database;

    protected String exclude;
    protected String include;

    protected int limit;
    protected int skip;
    protected String count;

    protected Query query;
    protected QueryOptions queryOptions;
    protected QueryResponse queryResponse;

    protected UriInfo uriInfo;
    protected HttpServletRequest httpServletRequest;
    protected MultivaluedMap<String, String> params;

    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

    protected long startTime;

    protected static Logger logger;

    /**
     * Loading properties file just one time to be more efficient. All methods
     * will check parameters so to avoid extra operations this config can load
     * versions and database
     */
    protected static BioNetDBConfiguration bioNetDBConfiguration;

    @Deprecated
    protected static NetworkDBAdaptor networkDBAdaptor;
    protected static Map<String, BioNetDBManager> bioNetDBManagers;

    private static final int LIMIT_DEFAULT = 1000;
    private static final int LIMIT_MAX = 5000;

    static {
        logger = LoggerFactory.getLogger("org.opencb.cellbase.server.ws.GenericRestWSServer");
        logger.info("Static block, creating Neo4JNetworkDBAdaptor");
        try {
            if (System.getenv("BIONETDB_HOME") != null) {
                logger.info("Loading configuration from '{}'", System.getenv("BIONETDB_HOME") + "/configuration.yml");
                bioNetDBConfiguration = BioNetDBConfiguration
                        .load(new FileInputStream(new File(System.getenv("BIONETDB_HOME") + "/configuration.yml")));
            } else {
                logger.info("Loading configuration from '{}'",
                        BioNetDBConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml").toString());
                bioNetDBConfiguration = BioNetDBConfiguration
                        .load(BioNetDBConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
            }

//            networkDBAdaptor = new Neo4JNetworkDBAdaptor("test1", bioNetDBConfiguration);
//        } catch (BioNetDBException e) {
//            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Init the manager map with the managers, this will allow methods to query the right database
        bioNetDBManagers = new HashMap<>();
        if (bioNetDBConfiguration != null && bioNetDBConfiguration.getDatabases().size() > 0) {
            try {
                for (DatabaseConfiguration databaseConfiguration : bioNetDBConfiguration.getDatabases()) {
                    bioNetDBManagers.put(databaseConfiguration.getId(),
                            new BioNetDBManager(databaseConfiguration.getId(), bioNetDBConfiguration));
                }
            } catch (BioNetDBException e) {
                e.printStackTrace();
            }
        }

        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectWriter = jsonObjectMapper.writer();
        logger.info("End of Static block");
    }


    public GenericRestWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                               @Context HttpServletRequest hsr) throws VersionException, DatabaseException {
        this.version = version;
        this.uriInfo = uriInfo;
        this.httpServletRequest = hsr;

        logger.debug("Executing GenericRestWSServer constructor with no Species");
        init(false);
    }

    @Deprecated
    public GenericRestWSServer(@PathParam("version") String version, @PathParam("database") String database, @Context UriInfo uriInfo,
                               @Context HttpServletRequest hsr) throws VersionException, DatabaseException {
        this.version = version;
        this.database = database;
        this.uriInfo = uriInfo;
        this.httpServletRequest = hsr;

        logger.debug("Executing GenericRestWSServer constructor");
        init(true);
    }


    protected void init(boolean checkSpecies) throws VersionException, DatabaseException {
        startTime = System.currentTimeMillis();

        queryResponse = new QueryResponse();
        queryOptions = new QueryOptions();

        checkPathParams(checkSpecies);
    }

    private void checkPathParams(boolean checkSpecies) throws VersionException, DatabaseException {
//        if (version == null) {
//            throw new VersionException("Version not valid: '" + version + "'");
//        }
//
//        if (checkSpecies && database == null) {
//            throw new DatabaseException("Species not valid: '" + database + "'");
//        }

        /**
         * Check version parameter, must be: v1, v2, ... If 'latest' then is
         * converted to appropriate version
         */
        if (version.equalsIgnoreCase("latest")) {
//            version = bioNetDBConfiguration.getVersion();
            logger.info("Version 'latest' detected, setting version parameter to '{}'", version);
        }

//        if (!bioNetDBConfiguration.getVersion().equalsIgnoreCase(this.version)) {
//            logger.error("Version '{}' does not match configuration '{}'", this.version, bioNetDBConfiguration.getVersion());
//            throw new VersionException("Version not valid: '" + version + "'");
//        }
    }

    public void parseQueryParams() {
        MultivaluedMap<String, String> multivaluedMap = uriInfo.getQueryParameters();
        queryOptions.put("metadata", (multivaluedMap.get("metadata") != null)
                ? multivaluedMap.get("metadata").get(0).equals("true")
                : true);

        if (exclude != null && !exclude.equals("")) {
            queryOptions.put("exclude", new LinkedList<>(Splitter.on(",").splitToList(exclude)));
        } else {
            queryOptions.put("exclude", (multivaluedMap.get("exclude") != null)
                    ? Splitter.on(",").splitToList(multivaluedMap.get("exclude").get(0))
                    : null);
        }

        if (include != null && !include.equals("")) {
            queryOptions.put("include", new LinkedList<>(Splitter.on(",").splitToList(include)));
        } else {
            queryOptions.put("include", (multivaluedMap.get("include") != null)
                    ? Splitter.on(",").splitToList(multivaluedMap.get("include").get(0))
                    : null);
        }

        queryOptions.put("limit", (limit > 0) ? Math.min(limit, LIMIT_MAX): LIMIT_DEFAULT);
        queryOptions.put("skip", (skip > 0) ? skip : -1);
        queryOptions.put("count", (count != null && !count.equals("")) ? Boolean.parseBoolean(count) : false);
//        outputFormat = (outputFormat != null && !outputFormat.equals("")) ? outputFormat : "json";

        // Now we add all the others QueryParams in the URL
        for (Map.Entry<String, List<String>> entry : multivaluedMap.entrySet()) {
            if (!queryOptions.containsKey(entry.getKey())) {
                logger.info("Adding '{}' to queryOptions", entry);
                queryOptions.put(entry.getKey(), entry.getValue().get(0));
            }
        }
    }


    @GET
    @Path("/help")
    public Response help() {
        return createOkResponse("No help available");
    }

//    @GET
//    public Response defaultMethod() {
//        switch (database) {
//            case "echo":
//                return createStringResponse("Status active");
//            default:
//                break;
//        }
//        return createOkResponse("Not valid option");
//    }


    protected Response createModelResponse(Class clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
            mapper.acceptJsonFormatVisitor(mapper.constructType(clazz), visitor);
            JsonSchema jsonSchema = visitor.finalSchema();

            return createOkResponse(jsonSchema);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected Response createErrorResponse(Exception e) {
        // First we print the exception in Server logs
        e.printStackTrace();

        // Now we prepare the response to client
        queryResponse = new QueryResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);
        queryResponse.setError(e.toString());

        QueryResult<ObjectMap> result = new QueryResult();
        result.setWarningMsg("Future errors will ONLY be shown in the QueryResponse body");
        result.setErrorMsg("DEPRECATED: " + e.toString());
        queryResponse.setResponse(Arrays.asList(result));

        return Response
                .fromResponse(createJsonResponse(queryResponse))
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .build();
    }

    protected Response createErrorResponse(String method, String errorMessage) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(
                    new HashMap<>().put("[ERROR] " + method, errorMessage)), MediaType.APPLICATION_JSON_TYPE));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected Response createOkResponse(Object obj) {
        queryResponse = new QueryResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);

        // Guarantee that the QueryResponse object contains a list of results
        List list;
        if (obj instanceof List) {
            list = (List) obj;
        } else {
            list = new ArrayList(1);
            list.add(obj);
        }
        queryResponse.setResponse(list);

        return createJsonResponse(queryResponse);
    }

    protected Response createOkResponse(Object obj, MediaType mediaType) {
        return buildResponse(Response.ok(obj, mediaType));
    }

    protected Response createOkResponse(Object obj, MediaType mediaType, String fileName) {
        return buildResponse(Response.ok(obj, mediaType).header("content-disposition", "attachment; filename =" + fileName));
    }

    protected Response createStringResponse(String str) {
        return buildResponse(Response.ok(str));
    }

    protected Response createJsonResponse(QueryResponse queryResponse) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(queryResponse), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("Error parsing queryResponse object");
            return createErrorResponse("", "Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    private Response buildResponse(Response.ResponseBuilder responseBuilder) {
        return responseBuilder
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "x-requested-with, content-type")
                .build();
    }

}
