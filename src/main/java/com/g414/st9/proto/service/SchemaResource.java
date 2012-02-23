package com.g414.st9.proto.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.g414.st9.proto.service.count.JDBICountService;
import com.g414.st9.proto.service.helper.AvailabilityManager;
import com.g414.st9.proto.service.helper.AvailabilityManager.ProtectedCommand;
import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.helper.Releasable;
import com.g414.st9.proto.service.helper.RunnableWithOutput;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.schema.CounterDefinition;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;
import com.g414.st9.proto.service.schema.SchemaHelper;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.sequence.SequenceServiceDatabaseImpl;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does CRUD operations for schema
 * definitions. TODO: perform better online management of database index tables.
 */
@Path("/1.0/s")
public class SchemaResource {
    private static final String SCHEMA_PREFIX = "$schema";

    @Inject
    protected AvailabilityManager availability;

    @Inject
    private KeyValueStorage store;

    @Inject
    protected SequenceServiceDatabaseImpl sequences;

    @Inject
    private JDBISecondaryIndex index;

    @Inject
    private JDBICountService counts;

    @Inject
    private IDBI database;

    private ObjectMapper mapper = new ObjectMapper();

    @POST
    @Path("{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO using String for the input value is busted/whack - pending better
    // automagical jackson configuration
    public Response createEntity(@PathParam("type") String type, String value)
            throws Exception {
        Integer typeId = getTypeIdPossiblyNull(type, true);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        Response exists = this.store.retrieve(SCHEMA_PREFIX + ":" + typeId,
                false);
        if (exists.getStatus() == 200) {
            return Response.status(Status.CONFLICT)
                    .entity("schema already exists").build();
        }

        SchemaDefinition schemaDefinition = null;

        try {
            if (value != null && value.length() > 0) {
                schemaDefinition = mapper.readValue(value,
                        SchemaDefinition.class);
            } else {
                schemaDefinition = SchemaHelper.getEmptySchema();
            }
        } catch (JsonMappingException e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("invalid schema definition json").build();
        }

        SchemaDefinitionValidator validator = new SchemaDefinitionValidator();
        try {
            validator.validate(schemaDefinition);
        } catch (ValidationException e) {
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
                    .build();
        }

        for (IndexDefinition indexDefinition : schemaDefinition.getIndexes()) {
            String indexName = indexDefinition.getName();

            index.createTable(database, type, indexName, schemaDefinition);
            index.createIndex(database, type, indexName, schemaDefinition);
        }

        for (CounterDefinition counterDefinition : schemaDefinition
                .getCounters()) {
            String counterName = counterDefinition.getName();

            counts.createTable(database, type, counterName, schemaDefinition);
        }

        return store.create(SCHEMA_PREFIX, value, typeId.longValue(), null,
                false);
    }

    @GET
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@PathParam("type") String type)
            throws Exception {
        Integer typeId = getTypeIdPossiblyNull(type, false);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        return store.retrieve(SCHEMA_PREFIX + ":" + typeId, false);
    }

    @PUT
    @Path("{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    // TODO using String for the input value is busted/whack - pending better
    // automagical jackson configuration
    public Response updateEntity(@PathParam("type") final String type,
            final String value,
            @QueryParam("doOutput") @DefaultValue("true") final boolean doOutput)
            throws Exception {
        final KeyValueStorage theStore = this.store;

        return availability.doProtected(new ProtectedCommand<Response>() {
            @Override
            public Response execute(final Releasable resource) throws Exception {
                final Integer typeId = getTypeIdPossiblyNull(type, false);

                if (typeId == null) {
                    return Response.status(Status.NOT_FOUND)
                            .entity("type not found").build();
                }

                Response existing = theStore.retrieve(SCHEMA_PREFIX + ":"
                        + typeId, false);
                if (existing.getStatus() != 200) {
                    return Response.status(Status.NOT_FOUND)
                            .entity("schema not found").build();
                }

                final SchemaDefinition original;

                try {
                    original = mapper.readValue((String) existing.getEntity(),
                            SchemaDefinition.class);
                } catch (JsonMappingException e) {
                    return Response.status(Status.BAD_REQUEST)
                            .entity("invalid schema definition json").build();
                }

                for (IndexDefinition indexDefinition : original.getIndexes()) {
                    String indexName = indexDefinition.getName();

                    if (index.tableExists(database, type, indexName)) {
                        index.dropTable(database, type, indexName);
                    }
                }

                for (CounterDefinition counterDefinition : original
                        .getCounters()) {
                    String counterName = counterDefinition.getName();

                    if (counts.tableExists(database, type, counterName)) {
                        counts.dropTable(database, type, counterName);
                    }
                }

                final SchemaDefinition schemaDefinition = mapper.readValue(
                        value, SchemaDefinition.class);

                SchemaDefinitionValidator validator = new SchemaDefinitionValidator();

                validator.validate(schemaDefinition);
                validator.validateUpdate(original, schemaDefinition);

                for (IndexDefinition indexDefinition : schemaDefinition
                        .getIndexes()) {
                    String indexName = indexDefinition.getName();

                    if (!index.tableExists(database, type, indexName)) {
                        index.createTable(database, type, indexName,
                                schemaDefinition);
                        index.createIndex(database, type, indexName,
                                schemaDefinition);
                    }
                }

                for (CounterDefinition counterDefinition : schemaDefinition
                        .getCounters()) {
                    String counterName = counterDefinition.getName();

                    if (!counts.tableExists(database, type, counterName)) {
                        counts.createTable(database, type, counterName,
                                schemaDefinition);
                    }
                }

                Response r = store.update(SCHEMA_PREFIX + ":" + typeId, value);
                if (r.getStatus() != 200) {
                    resource.release();

                    return r;
                }

                final RunnableWithOutput runnable = new RunnableWithOutput() {
                    @Override
                    public void run(final OutputStream output) {
                        database.withHandle(new HandleCallback<Void>() {
                            @Override
                            public Void withHandle(Handle handle)
                                    throws Exception {
                                try {
                                    rebuildIndexesAndCounters(handle, type,
                                            schemaDefinition, store.iterator(
                                                    type, schemaDefinition),
                                            output, doOutput);
                                    return null;
                                } catch (WebApplicationException e) {
                                    e.printStackTrace();
                                    printError(output, e.getMessage(), doOutput);

                                    return null;
                                }
                            }
                        });
                    }
                };

                if (doOutput) {
                    return Response.status(Status.OK)
                            .entity(new StreamingOutput() {
                                @Override
                                public void write(final OutputStream output)
                                        throws IOException,
                                        WebApplicationException {
                                    try {
                                        printOperationEvent(output,
                                                "$update_schema", "START",
                                                type, doOutput);

                                        runnable.run(output);

                                        printOperationEvent(output,
                                                "$update_schema", "END", type,
                                                doOutput);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        try {
                                            printError(output, e.getMessage(),
                                                    doOutput);
                                        } catch (Exception err) {
                                            err.printStackTrace();
                                        }
                                    } finally {
                                        resource.release();
                                    }
                                }
                            }).build();
                } else {
                    runnable.run(null);

                    return Response.status(Status.OK).entity("finished")
                            .build();
                }
            }
        });
    }

    @DELETE
    @Path("{type}")
    public Response deleteEntity(@PathParam("type") final String type)
            throws Exception {
        final KeyValueStorage theStore = this.store;

        return availability.doProtected(new ProtectedCommand<Response>() {
            @Override
            public Response execute(final Releasable resource) throws Exception {
                try {
                    Integer typeId = getTypeIdPossiblyNull(type, true);

                    if (typeId == null) {
                        return Response.status(Status.NOT_FOUND)
                                .entity("type not found").build();
                    }

                    Response existing = theStore.retrieve(SCHEMA_PREFIX + ":"
                            + typeId, false);
                    if (existing.getStatus() != 200) {
                        return Response.status(Status.NOT_FOUND)
                                .entity("schema not found").build();
                    }

                    final SchemaDefinition original = mapper.readValue(
                            (String) existing.getEntity(),
                            SchemaDefinition.class);

                    for (IndexDefinition indexDefinition : original
                            .getIndexes()) {
                        String indexName = indexDefinition.getName();

                        if (index.tableExists(database, type, indexName)) {
                            index.dropTable(database, type, indexName);
                        }
                    }

                    for (CounterDefinition counterDefinition : original
                            .getCounters()) {
                        String counterName = counterDefinition.getName();

                        if (counts.tableExists(database, type, counterName)) {
                            counts.dropTable(database, type, counterName);
                        }
                    }

                    return store.delete(SCHEMA_PREFIX + ":" + typeId, true);
                } finally {
                    resource.release();
                }
            }
        });
    }

    @POST
    @Path("{type}/rebuild")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response rebuild(@PathParam("type") final String type,
            @QueryParam("doOutput") @DefaultValue("true") final boolean doOutput) {
        return availability.doProtected(new ProtectedCommand<Response>() {
            @Override
            public Response execute(final Releasable resource) throws Exception {
                return Response.status(Status.OK).entity(new StreamingOutput() {
                    @Override
                    public void write(OutputStream output) throws IOException,
                            WebApplicationException {
                        Exception error = null;
                        try {
                            SchemaResource.this.rebuildSingle(type, output,
                                    doOutput);
                        } catch (Exception e) {
                            e.printStackTrace();
                            error = e;
                        } finally {
                            resource.release();
                            if (error != null) {
                                try {
                                    printError(output, error.getMessage(),
                                            doOutput);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }).build();
            }
        });
    }

    @POST
    @Path("rebuild_all")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response rebuildAll(
            @QueryParam("doOutput") @DefaultValue("true") final boolean doOutput)
            throws Exception {
        return availability.doProtected(new ProtectedCommand<Response>() {
            @Override
            public Response execute(final Releasable resource) throws Exception {
                return Response.status(Status.OK).entity(new StreamingOutput() {
                    @Override
                    public void write(OutputStream output) throws IOException,
                            WebApplicationException {
                        Exception error = null;
                        try {
                            printOperationEvent(output, "$rebuild_all",
                                    "START", "all", doOutput);

                            Iterator<Map<String, Object>> schemas = SchemaResource.this.store
                                    .iterator(SCHEMA_PREFIX);

                            while (schemas.hasNext()) {
                                Map<String, Object> schema = schemas.next();
                                String type = (String) schema.get("$type");
                                if (type == null || type.startsWith("$")) {
                                    continue;
                                }

                                SchemaResource.this.rebuildSingle(type, output,
                                        doOutput);
                            }

                            printOperationEvent(output, "$rebuild_all", "END",
                                    "all", doOutput);
                        } catch (Exception e) {
                            e.printStackTrace();
                            error = e;
                        } finally {
                            resource.release();
                            if (error != null) {
                                try {
                                    printError(output, error.getMessage(),
                                            doOutput);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }).build();
            }
        });
    }

    private void rebuildIndexesAndCounters(Handle handle, String type,
            SchemaDefinition schemaDefinition,
            Iterator<Map<String, Object>> instances, OutputStream output,
            boolean doOutput) throws Exception {
        for (IndexDefinition index : schemaDefinition.getIndexes()) {
            this.index.truncateTable(handle, type, index.getName());
        }

        for (CounterDefinition counterDefinition : schemaDefinition
                .getCounters()) {
            this.counts
                    .truncateTable(handle, type, counterDefinition.getName());
        }

        SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                schemaDefinition);

        long processed = 0;
        long successCount = 0;
        long skippedCount = 0;
        long failedCount = 0;

        while (instances.hasNext()) {
            Map<String, Object> notTransformed = instances.next();

            Key key = Key.valueOf((String) notTransformed.get("id"));
            Boolean deleted = (Boolean) notTransformed.get("$deleted");

            if (deleted != null && deleted.booleanValue()) {
                skippedCount += 1;
                continue;
            }

            Map<String, Object> instance = transformer
                    .validateTransform(notTransformed);

            try {
                for (IndexDefinition indexDef : schemaDefinition.getIndexes()) {
                    String indexName = indexDef.getName();

                    this.index.insertEntity(handle, key.getId(), instance,
                            type, indexName, schemaDefinition);
                }

                for (CounterDefinition counterDefinition : schemaDefinition
                        .getCounters()) {
                    String counterName = counterDefinition.getName();

                    this.counts.insertEntity(handle, key.getId(), instance,
                            type, counterName, schemaDefinition);
                }

                successCount += 1;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(instance);

                output.write((EncodingHelper.convertToJson(ImmutableMap
                        .<String, Object> of("$status", "OK", "$record",
                                "FAILED", "$data", instance)) + "\n")
                        .getBytes());
                output.flush();

                failedCount += 1;
            }

            processed += 1;

            if (processed % 100 == 0) {
                printStatus(output, type, successCount, skippedCount,
                        failedCount, doOutput);
            }
        }

        if (processed % 100 != 0) {
            printStatus(output, type, successCount, skippedCount, failedCount,
                    doOutput);
        }
    }

    private Response rebuildSingle(@PathParam("type") final String type,
            final OutputStream output, final boolean doOutput) throws Exception {
        Integer typeId = getTypeIdPossiblyNull(type, false);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        Response existing = this.store.retrieve(SCHEMA_PREFIX + ":" + typeId,
                false);
        if (existing.getStatus() != 200) {
            return Response.status(Status.NOT_FOUND).entity("schema not found")
                    .build();
        }

        final SchemaDefinition original;

        try {
            original = mapper.readValue((String) existing.getEntity(),
                    SchemaDefinition.class);
        } catch (JsonMappingException e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("invalid schema definition json").build();
        }

        for (IndexDefinition indexDefinition : original.getIndexes()) {
            String indexName = indexDefinition.getName();

            if (index.tableExists(database, type, indexName)) {
                index.dropTable(database, type, indexName);
            }
        }

        for (CounterDefinition counterDefinition : original.getCounters()) {
            String counterName = counterDefinition.getName();

            if (counts.tableExists(database, type, counterName)) {
                counts.dropTable(database, type, counterName);
            }
        }

        for (IndexDefinition indexDefinition : original.getIndexes()) {
            String indexName = indexDefinition.getName();

            if (!index.tableExists(database, type, indexName)) {
                index.createTable(database, type, indexName, original);
                index.createIndex(database, type, indexName, original);
            }
        }

        for (CounterDefinition counterDefinition : original.getCounters()) {
            String counterName = counterDefinition.getName();

            if (!counts.tableExists(database, type, counterName)) {
                counts.createTable(database, type, counterName, original);
            }
        }

        printOperationEvent(output, "$rebuild", "START", type, doOutput);

        database.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                try {
                    rebuildIndexesAndCounters(handle, type, original,
                            store.iterator(type, original), output, doOutput);
                    return null;
                } catch (WebApplicationException e) {
                    e.printStackTrace();

                    return null;
                }
            }
        });

        printOperationEvent(output, "$rebuild", "END", type, doOutput);

        return null;
    }

    private Integer getTypeIdPossiblyNull(String type, boolean val)
            throws Exception {
        try {
            return sequences.getTypeId(type, val);
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == Response.Status.BAD_REQUEST
                    .getStatusCode()) {
                return null;
            }

            throw e;
        }
    }

    private void printError(final OutputStream output, String error,
            boolean doOutput) throws IOException, Exception {
        if (!doOutput) {
            return;
        }

        output.write((EncodingHelper.convertToJson(ImmutableMap
                .<String, Object> of("$status", "ERROR", "$reason", error)) + "\n")
                .getBytes());
        output.flush();
    }

    private static void printOperationEvent(OutputStream output,
            String operation, String event, String type, boolean doOutput)
            throws Exception {
        if (!doOutput) {
            return;
        }

        output.write((EncodingHelper.convertToJson(ImmutableMap
                .<String, Object> of(
                        operation,
                        event,
                        "$date",
                        ISODateTimeFormat.basicDateTime().print(new DateTime()),
                        "$type", type)) + "\n").getBytes());
        output.flush();
    }

    private static void printStatus(OutputStream output, String type,
            Long successCount, Long skippedCount, Long failedCount,
            boolean doOutput) throws IOException, Exception {
        if (!doOutput) {
            return;
        }

        output.write((EncodingHelper
                .convertToJson(ImmutableMap
                        .<String, Object> builder()
                        .put("$status", "OK")
                        .put("$type", type)
                        .put("$date",
                                ISODateTimeFormat.basicDateTime().print(
                                        new DateTime()))
                        .put("$successCount", Long.valueOf(successCount))
                        .put("$skippedCount", Long.valueOf(skippedCount))
                        .put("$failedCount", Long.valueOf(failedCount)).build()) + "\n")
                .getBytes());
        output.flush();
    }
}
