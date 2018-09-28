package org.opencb.bionetdb.server.rest;

import io.swagger.annotations.Api;

import org.opencb.bionetdb.server.exception.VersionException;


import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

@Path("/{apiVersion}/analysis")
@Produces("application/json")
@Api(value = "Analysis", position = 1, description = "Methods for working with 'nodes'")
public class AnalysisWSServer extends GenericRestWSServer {

    public AnalysisWSServer(@Context UriInfo uriInfo,
                            @Context HttpServletRequest hsr) throws VersionException {
        super(uriInfo, hsr);
    }
}
