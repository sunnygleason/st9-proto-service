package com.g414.st9.proto.service;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * Service implementation of quarantine
 */
@Path("/1.0/q")
public class QuarantineResource {
    @Inject
    private KeyValueStorage store;

    @GET
    public Response badGetRequest() {
        return Response.status(Status.BAD_REQUEST)
                .entity("Missing entity 'id' in path").build();
    }

    @POST
    public Response badPostRequest() {
        return Response.status(Status.BAD_REQUEST)
                .entity("Missing entity 'type' in path").build();
    }

    @DELETE
    public Response badDeleteRequest() {
        return Response.status(Status.BAD_REQUEST)
                .entity("Missing entity 'id' in path").build();
    }

    @GET
    @Path("{key}")
    public Response isEntityQuarantined(@PathParam("key") String key)
            throws Exception {
        Response findResponse = store.retrieve(key, true);

        if (findResponse.getStatus() != 200) {
            return findResponse;
        }

        Map<String, Object> entity = EncodingHelper
                .parseJsonString((String) findResponse.getEntity());

        Boolean quarantined = (Boolean) entity.remove("$quarantined");

        Map<String, Object> response = new LinkedHashMap<String, Object>();
        response.put("id", entity.get("id"));
        response.put("$quarantined", quarantined != null && quarantined);

        return Response.status(Status.OK)
                .entity(EncodingHelper.convertToJson(response)).build();
    }

    @POST
    @Path("{key}")
    public Response quarantineEntity(@PathParam("key") String key)
            throws Exception {
        return store.setQuarantined(key, true);
    }

    @DELETE
    @Path("{key}")
    public Response unquarantineEntity(@PathParam("key") String key)
            throws Exception {
        return store.setQuarantined(key, false);
    }
}
