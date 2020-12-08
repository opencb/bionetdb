package org.opencb.bionetdb.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.google.common.base.Splitter;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ParamException;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.response.BioNetDBResult;
import org.opencb.bionetdb.core.response.RestResponse;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.server.exception.VersionException;
import org.opencb.commons.datastore.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by imedina on 01/10/15.
 */
@ApplicationPath("/")
@Path("/{apiVersion}")
@Produces(MediaType.APPLICATION_JSON)
public class GenericRestWSServer {

    @DefaultValue("")
    @PathParam("apiVersion")
    @ApiParam(name = "apiVersion", value = "Use 'latest' for last stable apiVersion", allowableValues = "v1", defaultValue = "v1")
    protected String apiVersion;

    protected String database;
//    protected String include;
//    protected String exclude;
//
//    protected int limit;
//    protected int skip;
//    protected boolean count;

    protected Query query;
    protected QueryOptions queryOptions;
//    protected QueryResponse queryResponse;

    protected UriInfo uriInfo;
    protected HttpServletRequest httpServletRequest;
    protected ObjectMap params;
    private String requestDescription;

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
    protected static BioNetDbManager bioNetDBManager;

    protected static AtomicBoolean initialized;

    private static final int LIMIT_DEFAULT = 1000;
    private static final int LIMIT_MAX = 5000;
    private static final int DEFAULT_LIMIT = 2000;
    private static final int MAX_LIMIT = 5000;

    static {
        initialized = new AtomicBoolean(false);

        logger = LoggerFactory.getLogger("org.opencb.bionetdb.server.rest.GenericRestWSServer");

        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectWriter = jsonObjectMapper.writer();
        logger.info("End of Static block");
    }

    public GenericRestWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws VersionException {
        this(uriInfo.getPathParameters().getFirst("apiVersion"), uriInfo, httpServletRequest);
    }

    public GenericRestWSServer(@PathParam("apiVersion") String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest hsr)
            throws VersionException {
        this.apiVersion = apiVersion;
        this.uriInfo = uriInfo;
        this.httpServletRequest = hsr;

        init();
    }

    private void init() throws VersionException {
        // This must be only executed once, this method loads the configuration and create the BioNetDBManagers
        if (initialized.compareAndSet(false, true)) {
            initBioNetDBObjects();
        }

        query = new Query();
        queryOptions = new QueryOptions();

        parseQueryParams();

        startTime = System.currentTimeMillis();
    }

    /**
     * This method loads the configuration and create the BioNetDB mangers, must be called once.
     */
    private void initBioNetDBObjects() {
        try {
            if (System.getenv("BIONETDB_HOME") != null) {
                logger.info("Loading configuration from '{}'", System.getenv("BIONETDB_HOME") + "/configuration.yml");
                bioNetDBConfiguration = BioNetDBConfiguration
                        .load(new FileInputStream(new File(System.getenv("BIONETDB_HOME") + "/configuration.yml")));
            } else {
                // We read 'BIONETDB_HOME' parameter from the web.xml file
                ServletContext context = httpServletRequest.getServletContext();
                String bionetdbHome = context.getInitParameter("BIONETDB_HOME");
                if (StringUtils.isNotEmpty(bionetdbHome)) {
                    logger.info("Loading configuration from '{}'", bionetdbHome + "/configuration.yml");
                    bioNetDBConfiguration = BioNetDBConfiguration
                            .load(new FileInputStream(new File(bionetdbHome + "/configuration.yml")));
                } else {
                    logger.info("Loading configuration from '{}'",
                            BioNetDBConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml").toString());
                    bioNetDBConfiguration = BioNetDBConfiguration
                            .load(BioNetDBConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Init the manager map with the managers, this will allow methods to query the right database
//        bioNetDBManagers = new HashMap<>();
        if (bioNetDBConfiguration != null) {
            try {
                bioNetDBManager = new BioNetDbManager(bioNetDBConfiguration);
            } catch (BioNetDBException e) {
                e.printStackTrace();
            }
        }
    }

    private void parseQueryParams() throws VersionException {
        // If by any reason 'apiVersion' is null we try to read it from the URI path, if not present an Exception is thrown
        if (this.apiVersion == null) {
            if (uriInfo.getPathParameters().containsKey("apiVersion")) {
                logger.warn("Setting 'apiVersion' from UriInfo object");
                this.apiVersion = uriInfo.getPathParameters().getFirst("apiVersion");
            } else {
                throw new VersionException("Version not valid: '" + apiVersion + "'");
            }
        }

        // Default database is the first one in the configuration file
//        if (bioNetDBConfiguration != null && bioNetDBConfiguration.getDatabases().size() > 0) {
//            this.database = bioNetDBConfiguration.getDatabases().get(0).getId();
//        }

        // We parse the rest of URL params
        MultivaluedMap<String, String> multivaluedMap = uriInfo.getQueryParameters();
        for (Map.Entry<String, List<String>> entry : multivaluedMap.entrySet()) {
            String value = entry.getValue().get(0);
            switch (entry.getKey()) {
                case QueryOptions.INCLUDE:
                case QueryOptions.EXCLUDE:
                    queryOptions.put(entry.getKey(), new LinkedList<>(Splitter.on(",").splitToList(value)));
                    break;
                case QueryOptions.LIMIT:
                    int limit = Integer.parseInt(value);
                    queryOptions.put(QueryOptions.LIMIT, (limit > 0) ? Math.min(limit, MAX_LIMIT) : DEFAULT_LIMIT);
                    break;
                case QueryOptions.SKIP:
                    int skip = Integer.parseInt(value);
                    queryOptions.put(entry.getKey(), (skip >= 0) ? skip : -1);
                    break;
                case QueryOptions.TIMEOUT:
                    queryOptions.put(entry.getKey(), Integer.parseInt(value));
                    break;
                case QueryOptions.SORT:
                case QueryOptions.ORDER:
                    queryOptions.put(entry.getKey(), value);
                    break;
                case QueryOptions.COUNT:
                    queryOptions.put(entry.getKey(), Boolean.parseBoolean(value));
                    break;
                case QueryOptions.SKIP_COUNT:
                    queryOptions.put(QueryOptions.SKIP_COUNT, Boolean.parseBoolean(value));
                    break;
                case "database":
                    this.database = value;
                    break;
                default:
                    // Query
                    query.put(entry.getKey(), value);
                    break;
            }
        }
    }


//    @GET
//    @Path("/help")
//    public Response help() {
//        return createOkResponse("No help available");
//    }

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

//    protected Response createErrorResponse(Exception e) {
//        // First we print the exception in Server logs
//        e.printStackTrace();
//
//        // Now we prepare the response to client
//        QueryResponse queryResponse = new QueryResponse();
//        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
//        queryResponse.setApiVersion(apiVersion);
//        queryResponse.setQueryOptions(queryOptions);
//        queryResponse.setError(e.toString());
//
//        QueryResult<ObjectMap> result = new QueryResult<>();
//        result.setWarningMsg("Future errors will ONLY be shown in the QueryResponse body");
//        result.setErrorMsg("DEPRECATED: " + e.toString());
//        queryResponse.setResponse(Collections.singletonList(result));
//
//        return Response
//                .fromResponse(createJsonResponse(queryResponse))
//                .status(Response.Status.INTERNAL_SERVER_ERROR)
//                .build();
//    }
//
//    protected Response createErrorResponse(String method, String errorMessage) {
//        try {
//            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(
//                    new HashMap<>().put("[ERROR] " + method, errorMessage)), MediaType.APPLICATION_JSON_TYPE));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    protected Response createErrorResponse(Throwable e) {
        return createErrorResponse(e, startTime, apiVersion, requestDescription, params);
    }

    public static Response createErrorResponse(Throwable e, long startTime, String apiVersion, String requestDescription,
                                               ObjectMap params) {
        // First we print the exception in Server logs
        logger.error("Catch error: " + e.getMessage(), e);

        // Now we prepare the response to client
        RestResponse<ObjectMap> queryResponse = new RestResponse<>();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(apiVersion);
        queryResponse.setParams(params);
        addErrorEvent(queryResponse, e);

        BioNetDBResult<ObjectMap> result = BioNetDBResult.empty();
        queryResponse.setResponses(Arrays.asList(result));

        Response.StatusType errorStatus;
        if (e instanceof WebApplicationException
                && ((WebApplicationException) e).getResponse() != null
                && ((WebApplicationException) e).getResponse().getStatusInfo() != null) {
            errorStatus = ((WebApplicationException) e).getResponse().getStatusInfo();
//        } else if (e instanceof CatalogAuthorizationException) {
//            errorStatus = Response.Status.FORBIDDEN;
//        } else if (e instanceof CatalogAuthenticationException) {
//            errorStatus = Response.Status.UNAUTHORIZED;
        } else {
            errorStatus = Response.Status.INTERNAL_SERVER_ERROR;
        }

        Response response = Response.fromResponse(createJsonResponse(queryResponse)).status(errorStatus).build();
//        logResponse(response.getStatusInfo(), queryResponse, startTime, requestDescription);
        return response;
    }

    protected Response createErrorResponse(String errorMessage, BioNetDBResult result) {
        RestResponse<ObjectMap> dataResponse = new RestResponse<>();
        dataResponse.setApiVersion(apiVersion);
        dataResponse.setParams(params);
        addErrorEvent(dataResponse, errorMessage);
        dataResponse.setResponses(Arrays.asList(result));

        Response response = Response.fromResponse(createJsonResponse(dataResponse)).status(Response.Status.INTERNAL_SERVER_ERROR).build();
//        logResponse(response.getStatusInfo(), dataResponse);
        return response;
    }

    protected Response createErrorResponse(String method, String errorMessage) {
        try {
            Response response = buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(new ObjectMap("error", errorMessage)),
                    MediaType.APPLICATION_JSON_TYPE));
//            logResponse(response.getStatusInfo());
            return response;
        } catch (JsonProcessingException e) {
//            e.printStackTrace();
            logger.error("Error creating error response", e);
        }

        return buildResponse(Response.ok("{\"error\":\"Error parsing json error\"}", MediaType.APPLICATION_JSON_TYPE));
    }

    static <T> void addErrorEvent(RestResponse<T> response, String message) {
        if (response.getEvents() == null) {
            response.setEvents(new ArrayList<>());
        }
        response.getEvents().add(new Event(Event.Type.ERROR, message));
    }

    private static <T> void addErrorEvent(RestResponse<T> response, Throwable e) {
        if (response.getEvents() == null) {
            response.setEvents(new ArrayList<>());
        }
        String message;
        if (e instanceof ParamException.QueryParamException && e.getCause() != null) {
            message = e.getCause().getMessage();
        } else {
            message = e.getMessage();
        }
        response.getEvents().add(
                new Event(Event.Type.ERROR, 0, e.getClass().getName(), e.getClass().getSimpleName(), message));
    }

    // TODO: Change signature
    //    protected <T> Response createOkResponse(BioNetDBResult<T> result)
    //    protected <T> Response createOkResponse(List<BioNetDBResult<T>> results)
    protected Response createOkResponse(Object obj) {
        return createOkResponse(obj, Collections.emptyList());
    }

    protected Response createOkResponse(Object obj, List<Event> events) {
        RestResponse queryResponse = new RestResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(apiVersion);
        queryResponse.setParams(params);
        queryResponse.setEvents(events);

        // Guarantee that the RestResponse object contains a list of results
        List<BioNetDBResult<?>> list = new ArrayList<>();
        if (obj instanceof List) {
            if (!((List) obj).isEmpty()) {
                Object firstObject = ((List) obj).get(0);
                if (firstObject instanceof BioNetDBResult) {
                    list = (List) obj;
                } else if (firstObject instanceof DataResult) {
                    List<DataResult> results = (List) obj;
                    // We will cast each of the DataResults to OpenCGAResult
                    for (DataResult result : results) {
                        list.add(new BioNetDBResult<>(result));
                    }
                } else {
                    list = Collections.singletonList(new BioNetDBResult<>(0, Collections.emptyList(), 1, (List) obj, 1));
                }
            }
        } else {
            if (obj instanceof BioNetDBResult) {
                list.add(((BioNetDBResult) obj));
            } else if (obj instanceof DataResult) {
                list.add(new BioNetDBResult<>((DataResult) obj));
            } else {
                list.add(new BioNetDBResult<>(0, Collections.emptyList(), 1, Collections.singletonList(obj), 1));
            }
        }
        queryResponse.setResponses(list);

        Response response = createJsonResponse(queryResponse);
//        logResponse(response.getStatusInfo(), queryResponse);
        return response;
    }

//    protected Response createOkResponse(Object obj) {
//        QueryResponse queryResponse = new QueryResponse();
//        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
//        queryResponse.setApiVersion(apiVersion);
//        queryResponse.setQueryOptions(queryOptions);
//
//        // Guarantee that the QueryResponse object contains a list of results
//        List list;
//        if (obj instanceof List) {
//            list = (List) obj;
//        } else {
//            list = new ArrayList(1);
//            list.add(obj);
//        }
//        queryResponse.setResponse(list);
//        return createJsonResponse(queryResponse);
//    }

    protected Response createOkResponse(Object obj, MediaType mediaType) {
        return buildResponse(Response.ok(obj, mediaType));
    }

    protected Response createOkResponse(Object obj, MediaType mediaType, String fileName) {
        return buildResponse(Response.ok(obj, mediaType).header("content-disposition", "attachment; filename =" + fileName));
    }

    protected Response createStringResponse(String str) {
        return buildResponse(Response.ok(str));
    }

    static Response createJsonResponse(RestResponse queryResponse) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(queryResponse), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            logger.error("Error parsing queryResponse object", e);
            throw new WebApplicationException("Error parsing queryResponse object", e);
        }
    }

    protected static Response buildResponse(Response.ResponseBuilder responseBuilder) {
        return responseBuilder
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "x-requested-with, content-type, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                .build();
    }
//    protected Response createJsonResponse(QueryResponse queryResponse) {
//        try {
//            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(queryResponse), MediaType.APPLICATION_JSON_TYPE));
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//            logger.error("Error parsing queryResponse object");
//            return createErrorResponse("", "Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
//        }
//    }
//
//    private Response buildResponse(Response.ResponseBuilder responseBuilder) {
//        return responseBuilder
//                .header("Access-Control-Allow-Origin", "*")
//                .header("Access-Control-Allow-Headers", "x-requested-with, content-type, authorization")
//                .header("Access-Control-Allow-Credentials", "true")
//                .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
//                .build();
//    }
}
