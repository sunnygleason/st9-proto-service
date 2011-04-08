package com.g414.st9.proto.service;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * Nukes everything in the store.
 */
@Path("/1.0/nuke")
public class NukeResource {
    @Inject
    private KeyValueStorage store;

    @POST
    public Response nuke(@QueryParam("preserveSchema") Boolean preserveSchema)
            throws Exception {
        boolean preserve = (preserveSchema != null) && preserveSchema;

        return store.clearRequested(preserve);
    }
}
