package com.g414.st9.proto.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.common.collect.ImmutableMap;
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
    public Response importAll(final InputStream postBody) throws Exception {
        Response clearResponse = this.store.clearRequested(false);

        if (clearResponse.getStatus() != Status.NO_CONTENT.getStatusCode()) {
            return clearResponse;
        }

        return Response.status(Status.OK).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException,
                    WebApplicationException {
                Scanner scan = null;
                try {
                    long successCount = 0;
                    long skippedCount = 0;
                    long failedCount = 0;

                    scan = new Scanner(postBody);

                    try {
                        output.write((EncodingHelper.convertToJson(ImmutableMap
                                .<String, Object> of("$export", "START")) + "\n")
                                .getBytes());
                        output.flush();
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }

                    long processed = 0;

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

                                Map<String, Object> special = extractSpecialFields(parsedObject);

                                Boolean deleted = (Boolean) special
                                        .get("$deleted");
                                Boolean quarantined = (Boolean) special
                                        .get("$quarantined");

                                Key key = Key.valueOf((String) parsedObject
                                        .remove("id"));
                                Long version = (parsedObject
                                        .containsKey("version")) ? Long
                                        .parseLong((String) parsedObject
                                                .remove("version")) : 1L;

                                String encId = key.getEncryptedIdentifier();

                                Map<String, Object> object = new LinkedHashMap<String, Object>();
                                object.put("id", encId);
                                object.put("kind", key.getType());
                                object.put("version", version.toString());
                                object.putAll(parsedObject);

                                Response r = null;

                                if (key.getType().equals("$schema")) {
                                    String type = (String) special.get("$type");

                                    r = schema.createEntity(type,
                                            EncodingHelper
                                                    .convertToJson(object));
                                    successCount += 1;
                                } else if (!key.getType().startsWith("$")) {
                                    String jsonValue = EncodingHelper
                                            .convertToJson(object);

                                    if ((deleted != null) && deleted) {
                                        r = store.createDeleted(key.getType(),
                                                key.getId());
                                        jsonValue = instance;
                                    } else {
                                        r = store.create(key.getType(),
                                                jsonValue, key.getId(),
                                                version, true);

                                        if ((quarantined != null)
                                                && quarantined) {
                                            store.setQuarantined(encId, true);
                                        }
                                    }

                                    if (r.getStatus() != Status.OK
                                            .getStatusCode()) {
                                        System.out.println(instance);

                                        output.write((EncodingHelper.convertToJson(ImmutableMap.<String, Object> of(
                                                "$status", "OK", "$record",
                                                "FAILED", "$id",
                                                key.getEncryptedIdentifier(),
                                                "$code", r.getStatus(),
                                                "$reason", r.getEntity())) + "\n")
                                                .getBytes());

                                        failedCount += 1;
                                        continue;
                                    }

                                    if (r.getEntity().equals(jsonValue)) {
                                        successCount += 1;
                                    } else {
                                        throw new RuntimeException(
                                                "mismatched entity : "
                                                        + key.getEncryptedIdentifier());
                                    }
                                } else {
                                    output.write((EncodingHelper.convertToJson(ImmutableMap.<String, Object> of(
                                            "$status", "OK", "$record",
                                            "SKIPPED", "$id",
                                            key.getEncryptedIdentifier())) + "\n")
                                            .getBytes());
                                    skippedCount += 1;

                                    continue;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println(instance);

                                output.write((EncodingHelper
                                        .convertToJson(ImmutableMap
                                                .<String, Object> of("$status",
                                                        "OK", "$record",
                                                        "FAILED", "$data",
                                                        instance)) + "\n")
                                        .getBytes());
                                failedCount += 1;
                            }

                            processed += 1;

                            if (processed % 100 == 0) {
                                printStatus(output, successCount, skippedCount,
                                        failedCount);
                            }
                        }
                    }

                    printStatus(output, successCount, skippedCount, failedCount);

                    try {
                        output.write((EncodingHelper.convertToJson(ImmutableMap
                                .<String, Object> of("$export", "END")) + "\n")
                                .getBytes());
                        output.flush();
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        output.write((EncodingHelper.convertToJson(ImmutableMap
                                .<String, Object> of("$status", "ERROR",
                                        "$reason", e.getMessage())) + "\n")
                                .getBytes());
                        output.flush();
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }

                    return;
                } finally {
                    if (scan != null) {
                        scan.close();
                    }
                }
            }
        }).build();
    }

    private static Map<String, Object> extractSpecialFields(
            Map<String, Object> parsedObject) {
        Map<String, Object> special = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, Object> entry : parsedObject.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("$")) {
                special.put(entry.getKey(), entry.getValue());
            }
        }

        for (String key : special.keySet()) {
            parsedObject.remove(key);
        }

        return special;
    }

    private static void printStatus(OutputStream output, Long successCount,
            Long skippedCount, Long failedCount) throws IOException, Exception {
        output.write((EncodingHelper.convertToJson(ImmutableMap
                .<String, Object> of(
                        "$status",
                        "OK",
                        "$date",
                        ISODateTimeFormat.basicDateTime().print(new DateTime()),
                        "$successCount", Long.valueOf(successCount),
                        "$skippedCount", Long.valueOf(skippedCount),
                        "$failedCount", Long.valueOf(failedCount))) + "\n")
                .getBytes());
        output.flush();
    }
}
