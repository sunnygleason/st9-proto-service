package com.g414.st9.proto.service;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.g414.st9.proto.service.helper.AvailabilityManagementFilter;
import com.g414.st9.proto.service.helper.AvailabilityManager;
import com.google.inject.Inject;

/**
 * Controls availability of the service. NOTE: you must pass the __FORCE__=true
 * parameter to bypass the availability filter once set.
 * 
 * @see AvailabilityManagementFilter
 */
@Path("/1.0/a")
public class AvailabilityResource {
    @Inject
    private AvailabilityManager availability;

    @POST
    @Path("on")
    public Response setOn() throws Exception {
        availability.setAvailable(true);

        return Response.status(Status.NO_CONTENT).build();
    }

    @POST
    @Path("off")
    public Response setOff() throws Exception {
        availability.setAvailable(false);

        return Response.status(Status.NO_CONTENT).build();
    }
}
