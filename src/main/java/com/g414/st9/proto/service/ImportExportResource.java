package com.g414.st9.proto.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.g414.st9.proto.service.store.EncodingHelper;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does import / export operations for
 * KV pairs.
 */
@Path("/1.0/x")
public class ImportExportResource {
    @Inject
    private KeyValueStorage store;

    @Inject
    private SchemaResource schema;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response exportAll() throws Exception {
        return store.exportAll();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO using String for the input value is busted/whack - pending better
    // automagical jackson configuration
    public Response importAll(String value) throws Exception {
        this.store.clearRequested(false);

        String[] values = value.split("\n");

        List<String> success = new ArrayList<String>();
        List<String> skipped = new ArrayList<String>();
        List<String> failed = new ArrayList<String>();

        for (String instance : values) {
            if (instance.trim().length() == 0) {
                continue;
            }

            try {
                Map<String, Object> object = EncodingHelper
                        .parseJsonString(instance);
                Boolean deleted = (Boolean) object.remove("$deleted");

                Key key = Key.valueOf((String) object.get("id"));

                Response r = null;

                if (key.getType().equals("$schema")) {
                    String type = (String) object.remove("$type");

                    r = schema.createEntity(type,
                            EncodingHelper.convertToJson(object));

                    success.add(key.getEncryptedIdentifier());
                } else if (!key.getType().startsWith("$")) {
                    r = store.create(key.getType(),
                            EncodingHelper.convertToJson(object), key.getId(),
                            true);

                    if (r.getStatus() != Status.OK.getStatusCode()) {
                        failed.add(instance);
                        continue;
                    }

                    if (deleted != null && deleted) {
                        store.delete(key.getIdentifier());
                    } else if (r.getEntity().equals(instance)) {
                        // perfect!
                    } else {
                        throw new RuntimeException("mismatched entity : "
                                + key.getEncryptedIdentifier());
                    }

                    success.add(key.getEncryptedIdentifier());
                } else {
                    skipped.add(key.getEncryptedIdentifier());

                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
                failed.add(instance);
            }
        }

        Map<String, Object> result = new LinkedHashMap<String, Object>();

        result.put("success", success);
        result.put("skipped", skipped);
        result.put("failed", failed);

        return Response.status(Status.OK)
                .entity(EncodingHelper.convertToJson(result)).build();
    }
}
