package com.g414.st9.proto.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does CRUD operations for KV pairs.
 * You may ask yourself - why create a silly class that just dispatches? For the
 * answer, stick around...
 */
@Path("/1.0/e")
public class KeyValueResource {
    @Inject
    private KeyValueStorage store;

    @POST
    @Path("{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO using String for the input value is busted/whack - pending better
    // automagical jackson configuration
    public Response createEntity(@PathParam("type") String type, String value)
            throws Exception {
        return store.create(type, value, null, true);
    }

    @GET
    @Path("{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@PathParam("key") String key)
            throws Exception {
        return store.retrieve(key);
    }

    @GET
    @Path("multi")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@QueryParam("k") List<String> keys)
            throws Exception {
        return store.multiRetrieve(keys);
    }

    @PUT
    @Path("{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    // TODO using String for the input value is busted/whack - pending better
    // automagical jackson configuration
    public Response updateEntity(@PathParam("key") String key, String value)
            throws Exception {
        return store.update(key, value);
    }

    @DELETE
    @Path("{key}")
    public Response deleteEntity(@PathParam("key") String key) throws Exception {
        return store.delete(key);
    }

    /** Just for testing of course... */
    public void clear() {
        store.clear();
    }

    /** Just for testing of course... */
    public Iterator<Map<String, Object>> iterator(String type) throws Exception {
        return store.iterator(type);
    }
}
