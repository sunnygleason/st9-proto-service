package com.g414.st9.proto.service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.g414.st9.proto.service.helper.EncodingHelper;
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
    public Response importAll(InputStream postBody) throws Exception {
        Scanner scan = null;
        try {
            Response clearResponse = this.store.clearRequested(false);

            if (clearResponse.getStatus() != Status.NO_CONTENT.getStatusCode()) {
                return clearResponse;
            }

            List<String> success = new ArrayList<String>();
            List<String> skipped = new ArrayList<String>();
            List<String> failed = new ArrayList<String>();

            scan = new Scanner(postBody);

            while (scan.hasNextLine()) {
                String value = scan.nextLine();
                String[] values = value.split("\n");

                for (String instance : values) {
                    if (instance.trim().length() == 0) {
                        continue;
                    }

                    try {
                        Map<String, Object> parsedObject = EncodingHelper
                                .parseJsonString(instance);

                        if (parsedObject.containsKey("$export")) {
                            continue;
                        }

                        Boolean deleted = (Boolean) parsedObject
                                .remove("$deleted");

                        Key key = Key.valueOf((String) parsedObject
                                .remove("id"));
                        Long version = (parsedObject.containsKey("version")) ? Long
                                .parseLong((String) parsedObject
                                        .remove("version")) : 1L;

                        Map<String, Object> object = new LinkedHashMap<String, Object>();
                        object.put("id", key.getEncryptedIdentifier());
                        object.put("kind", key.getType());
                        object.put("version", version.toString());
                        object.putAll(parsedObject);

                        Response r = null;

                        if (key.getType().equals("$schema")) {
                            String type = (String) object.remove("$type");

                            r = schema.createEntity(type,
                                    EncodingHelper.convertToJson(object));

                            success.add(key.getEncryptedIdentifier());
                        } else if (!key.getType().startsWith("$")) {
                            String jsonValue = EncodingHelper
                                    .convertToJson(object);

                            if (deleted != null && deleted) {
                                r = store.createDeleted(key.getType(),
                                        key.getId());
                                jsonValue = instance;
                            } else {
                                r = store.create(key.getType(), jsonValue,
                                        key.getId(), version, true);
                            }

                            if (r.getStatus() != Status.OK.getStatusCode()) {
                                failed.add(instance);
                                continue;
                            }

                            if (r.getEntity().equals(jsonValue)) {
                                success.add(key.getEncryptedIdentifier());
                            } else {
                                throw new RuntimeException(
                                        "mismatched entity : "
                                                + key.getEncryptedIdentifier());
                            }
                        } else {
                            skipped.add(key.getEncryptedIdentifier());

                            continue;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println(instance);
                        failed.add(instance);
                    }
                }
            }

            Map<String, Object> result = new LinkedHashMap<String, Object>();

            result.put("success", success);
            result.put("skipped", skipped);
            result.put("failed", failed);

            return Response
                    .status((skipped.isEmpty() && failed.isEmpty()) ? Status.OK
                            : Status.BAD_REQUEST)
                    .entity(EncodingHelper.convertToJson(result)).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).build();
        } finally {
            if (scan != null) {
                scan.close();
            }
        }
    }
}
