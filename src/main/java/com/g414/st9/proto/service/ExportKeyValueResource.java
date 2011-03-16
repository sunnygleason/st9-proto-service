package com.g414.st9.proto.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does export operations for KV pairs.
 */
@Path("/1.0/x")
public class ExportKeyValueResource {
    @Inject
    private KeyValueStorage store;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response retrieveEntity() throws Exception {
        return store.exportAll();
    }
}
