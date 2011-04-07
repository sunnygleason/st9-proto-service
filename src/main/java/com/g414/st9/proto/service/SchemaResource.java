package com.g414.st9.proto.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does CRUD operations for schema
 * definitions. TODO: perform better online management of database index tables.
 */
@Path("/1.0/s")
public class SchemaResource {
    private static final String SCHEMA_PREFIX = "$schema";

    @Inject
    private KeyValueStorage store;

    @Inject
    private JDBISecondaryIndex index;

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
        Integer typeId = store.getTypeId(type);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        SchemaDefinition schemaDefinition = mapper.readValue(value,
                SchemaDefinition.class);

        SchemaDefinitionValidator validator = new SchemaDefinitionValidator();
        validator.validate(schemaDefinition);

        for (IndexDefinition indexDefinition : schemaDefinition.getIndexes()) {
            String indexName = indexDefinition.getName();

            index.createTable(database, type, indexName, schemaDefinition);
            index.createIndex(database, type, indexName, schemaDefinition);
        }

        return store.create(SCHEMA_PREFIX, value, typeId.longValue(), false);
    }

    @GET
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@PathParam("type") String type)
            throws Exception {
        Integer typeId = store.getTypeId(type);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        return store.retrieve(SCHEMA_PREFIX + ":" + typeId);
    }

    @PUT
    @Path("{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    // TODO using String for the input value is busted/whack - pending better
    // automagical jackson configuration
    public Response updateEntity(@PathParam("type") final String type,
            final String value) throws Exception {
        Integer typeId = store.getTypeId(type);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        final SchemaDefinition schemaDefinition = mapper.readValue(value,
                SchemaDefinition.class);

        SchemaDefinitionValidator validator = new SchemaDefinitionValidator();
        validator.validate(schemaDefinition);

        for (IndexDefinition indexDefinition : schemaDefinition.getIndexes()) {
            String indexName = indexDefinition.getName();

            if (!index.indexExists(database, type, indexName)) {
                index.createTable(database, type, indexName, schemaDefinition);
                index.createIndex(database, type, indexName, schemaDefinition);
            }
        }

        database.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                try {
                    index.rebuildIndexes(handle, type, schemaDefinition,
                            store.iterator(type, schemaDefinition));
                    return null;
                } catch (WebApplicationException e) {
                    e.printStackTrace();

                    return null;
                }
            }
        });

        store.delete(SCHEMA_PREFIX + ":" + typeId);

        return store.update(SCHEMA_PREFIX + ":" + typeId, value);
    }

    @DELETE
    @Path("{type}")
    public Response deleteEntity(@PathParam("type") String type)
            throws Exception {
        Integer typeId = store.getTypeId(type);

        if (typeId == null) {
            return Response.status(Status.NOT_FOUND).entity("type not found")
                    .build();
        }

        // TODO: actually delete schemas / indexes

        return store.delete(SCHEMA_PREFIX + ":" + typeId);
    }
}
