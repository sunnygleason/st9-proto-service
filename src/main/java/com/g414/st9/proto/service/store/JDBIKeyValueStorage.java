package com.g414.st9.proto.service.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleRegistration;
import com.g414.guice.lifecycle.LifecycleSupportBase;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaHelper;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;

/**
 * Abstract implementation of key-value storage based on JDBI.
 */
public abstract class JDBIKeyValueStorage implements KeyValueStorage,
        LifecycleRegistration {
    @Inject
    protected IDBI database;

    @Inject
    protected KeyValueCache cache;

    @Inject
    protected JDBISecondaryIndex index;

    @Inject
    protected CounterService counters;

    protected abstract String getPrefix();

    protected final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public void register(Lifecycle lifecycle) {
        lifecycle.register(new LifecycleSupportBase() {
            @Override
            public void init() {
                JDBIKeyValueStorage.this.initialize();
            }
        });
    }

    public void initialize() {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                performInitialization(handle);

                return null;
            }
        });
    }

    @Override
    public Integer getTypeId(final String type) throws Exception {
        return counters.getTypeId(type);
    }

    public String getTypeName(final Integer id) throws Exception {
        return counters.getTypeName(id);
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#create(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response create(final String type, String inValue, final Long id,
            boolean strictType) throws Exception {
        if (type == null
                || (strictType && ((type.contains("@") || type.contains("$"))))) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'type'").build();
        }

        final Map<String, Object> readValue;

        try {
            readValue = EncodingHelper.parseJsonString(inValue);
        } catch (WebApplicationException e) {
            return e.getResponse();
        }

        readValue.remove("id");
        readValue.remove("kind");

        final SchemaDefinition definition;
        final Key newKey;

        try {
            definition = loadOrCreateEmptySchemaOutsideTxn(counters
                    .getTypeId(type));
            if (id != null) {
                newKey = new Key(type, id);
                counters.bumpKey(type, id);
            } else {
                newKey = counters.nextKey(type);
            }
        } catch (Exception e) {
            return getErrorResponse(e);
        }

        final String valueJson = getDisplayJson(newKey, readValue);

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    Map<String, Object> toInsert = readValue;
                    toInsert = applySchema(definition, toInsert);

                    for (IndexDefinition indexDef : definition.getIndexes()) {
                        index.insertEntity(handle, newKey.getId(), toInsert,
                                type, indexDef.getName(), definition);
                    }

                    byte[] valueBytes = getStorableSmileLzf(toInsert);

                    int inserted = doInsert(handle, getTypeId(type),
                            newKey.getId(), valueBytes);

                    if (inserted > 0) {
                        cache.put(EncodingHelper.toKVCacheKey(newKey
                                .getIdentifier()), valueBytes);
                    }

                    return (inserted == 1) ? Response.status(Status.OK)
                            .entity(valueJson).build() : Response
                            .status(Status.INTERNAL_SERVER_ERROR)
                            .entity("Entity not inserted").build();
                } catch (WebApplicationException e) {
                    return e.getResponse();
                } catch (Exception e) {
                    return getErrorResponse(e);
                }
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#retrieve(java.lang.String)
     */
    @Override
    public Response retrieve(final String key) throws Exception {
        try {
            KeyHelper.validateKey(key);
            Key realKey = Key.valueOf(key);

            byte[] objectBytes = getObjectBytes(realKey);
            if (objectBytes == null) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            return makeRetrieveResponse((String) realKey.getType(), key,
                    objectBytes,
                    loadOrCreateEmptySchemaOutsideTxn(getTypeId(realKey
                            .getType())));
        } catch (WebApplicationException e) {
            return e.getResponse();
        } catch (Exception e) {
            return getErrorResponse(e);
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#multiRetrieve(List)
     */
    @Override
    public Response multiRetrieve(final List<String> keys) throws Exception {
        if (keys == null || keys.isEmpty()) {
            return Response
                    .status(Status.OK)
                    .entity(EncodingHelper.convertToJson(Collections
                            .<String, Object> emptyMap())).build();
        }

        if (keys.size() > 100) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Multiget max is 100 keys").build();
        }

        final Map<String, SchemaDefinition> schemaDefinitions = new HashMap<String, SchemaDefinition>();

        final List<String> cacheKeys = new ArrayList<String>();
        try {
            for (String key : keys) {
                Key realKey = Key.valueOf(key);
                cacheKeys.add(EncodingHelper.toKVCacheKey(key));

                if (!schemaDefinitions.containsKey(realKey.getType())) {
                    schemaDefinitions.put(realKey.getType(),
                            loadOrCreateEmptySchemaOutsideTxn(getTypeId(realKey
                                    .getType())));
                }
            }
        } catch (WebApplicationException e) {
            return e.getResponse();
        }

        final Map<String, byte[]> maybeFound;
        try {
            maybeFound = cache.multiget(cacheKeys);
        } catch (Exception e) {
            return getErrorResponse(e);
        }

        final Map<String, byte[]> cacheFound = new HashMap<String, byte[]>();
        final List<String> notFound = new ArrayList<String>();

        for (int i = 0; i < cacheKeys.size(); i++) {
            String cacheKey = cacheKeys.get(i);
            String origKey = keys.get(i);

            byte[] value = maybeFound.get(cacheKey);
            if (value == null) {
                notFound.add(origKey);
            } else {
                cacheFound.put(origKey, value);
            }
        }

        if (notFound.isEmpty()) {
            return makeMultiRetrieveResponse(keys, cacheFound,
                    Collections.<String, Object> emptyMap(),
                    Collections.<String, SchemaDefinition> emptyMap());
        }

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    Map<String, Object> dbFound = new HashMap<String, Object>();

                    for (String key : notFound) {
                        KeyHelper.validateKey(key);
                        Key realKey = Key.valueOf(key);

                        final int typeId = counters.getTypeId(realKey.getType());
                        final long keyId = realKey.getId();

                        Query<Map<String, Object>> select = handle
                                .createQuery(getPrefix() + "retrieve");

                        select.bind("key_type", typeId);
                        select.bind("key_id", keyId);

                        List<Map<String, Object>> results = select.list();

                        if (results == null || results.isEmpty()) {
                            dbFound.put(key, null);

                            continue;
                        }

                        byte[] valueBytesLzf = (byte[]) results.iterator()
                                .next().get("_value");
                        LinkedHashMap<String, Object> found = (LinkedHashMap<String, Object>) EncodingHelper
                                .parseSmileLzf(valueBytesLzf);
                        found.remove("id");
                        found.remove("kind");

                        LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
                        value.put("id", Key.valueOf(key)
                                .getEncryptedIdentifier());
                        value.put("kind", realKey.getType());
                        value.putAll(found);

                        dbFound.put(key, value);

                        cache.put(EncodingHelper.toKVCacheKey(realKey
                                .getIdentifier()), valueBytesLzf);
                    }

                    return makeMultiRetrieveResponse(keys, cacheFound, dbFound,
                            schemaDefinitions);
                } catch (WebApplicationException e) {
                    return e.getResponse();
                } catch (Exception e) {
                    return getErrorResponse(e);
                }
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#update(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response update(final String key, String inValue) throws Exception {
        final Map<String, Object> readValue;

        try {
            KeyHelper.validateKey(key);
            readValue = EncodingHelper.parseJsonString(inValue);
        } catch (Exception e) {
            return getErrorResponse(e);
        }

        final Key realKey = Key.valueOf(key);
        final SchemaDefinition definition = loadOrCreateEmptySchemaOutsideTxn(getTypeId(realKey
                .getType()));

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    final int typeId = counters.getTypeId(realKey.getType());
                    final long keyId = realKey.getId();

                    Map<String, Object> toUpdate = readValue;

                    toUpdate = applySchema(definition, toUpdate);

                    for (IndexDefinition indexDef : definition.getIndexes()) {
                        index.updateEntity(handle, Long.valueOf(keyId),
                                toUpdate, realKey.getType(),
                                indexDef.getName(), definition);
                    }

                    String valueJson = getDisplayJson(realKey, readValue);
                    byte[] valueBytes = getStorableSmileLzf(toUpdate);

                    int updated = doUpdate(handle, typeId, keyId, valueBytes);

                    if (updated > 0) {
                        cache.put(EncodingHelper.toKVCacheKey(realKey
                                .getIdentifier()), valueBytes);
                    }

                    return (updated == 0) ? Response.status(Status.NOT_FOUND)
                            .entity("").build() : Response.status(Status.OK)
                            .entity(valueJson).build();
                } catch (WebApplicationException e) {
                    return e.getResponse();
                } catch (Exception e) {
                    return getErrorResponse(e);
                }
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#delete(java.lang.String)
     */
    @Override
    public Response delete(final String key) throws Exception {
        final Key realKey;

        try {
            realKey = Key.valueOf(key);
        } catch (Exception e) {
            return getErrorResponse(e);
        }

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    final int typeId = counters.getTypeId(realKey.getType());
                    final long keyId = realKey.getId();

                    SchemaDefinition definition = loadOrCreateEmptySchema(
                            handle, typeId);

                    for (IndexDefinition indexDef : definition.getIndexes()) {
                        index.deleteEntity(handle, Long.valueOf(keyId),
                                realKey.getType(), indexDef.getName());
                    }

                    Update delete = handle.createStatement(getPrefix()
                            + "delete");
                    delete.bind("updated_dt", getEpochSecondsNow());
                    delete.bind("key_type", typeId);
                    delete.bind("key_id", keyId);

                    int deleted = delete.execute();

                    if (deleted > 0) {
                        cache.delete(EncodingHelper.toKVCacheKey(realKey
                                .getIdentifier()));
                    }

                    return (deleted == 0) ? Response.status(Status.NOT_FOUND)
                            .entity("").build() : Response
                            .status(Status.NO_CONTENT).entity("").build();
                } catch (WebApplicationException e) {
                    return e.getResponse();
                } catch (IllegalArgumentException e) {
                    return getErrorResponse(e);
                }
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#clear()
     */
    @Override
    public void clear() {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                try {
                    index.clear(handle, iterator("$schema"));

                    handle.createStatement(getPrefix() + "truncate_key_types")
                            .execute();
                    handle.createStatement(getPrefix() + "truncate_sequences")
                            .execute();
                    handle.createStatement(getPrefix() + "truncate_key_values")
                            .execute();

                    performInitialization(handle);

                    return null;
                } catch (Exception e) {
                    e.printStackTrace();

                    throw new RuntimeException(e);
                }
            }
        });

        cache.clear();
        counters.clear();
    }

    @Override
    public Iterator<Map<String, Object>> iterator(String type) throws Exception {
        return iterator(type,
                loadOrCreateEmptySchemaOutsideTxn(getTypeId(type)));
    }

    @Override
    public Iterator<Map<String, Object>> iterator(final String type,
            final SchemaDefinition schemaDefinition) throws Exception {
        return new Iterator<Map<String, Object>>() {
            private final SchemaDefinition definition = schemaDefinition;
            private final Iterator<String> inner = createKeyIterator(type);
            private String nextKey = advance();

            public String advance() {
                String key = null;

                while (key == null && inner.hasNext()) {
                    String newKey = inner.next();
                    KeyHelper.validateKey(newKey);

                    final Key realKey;
                    try {
                        realKey = Key.valueOf(newKey);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    if (type.equals(realKey.getType())) {
                        key = newKey;
                        break;
                    }
                }

                return key;
            }

            @Override
            public boolean hasNext() {
                return nextKey != null;
            }

            @Override
            public Map<String, Object> next() {
                try {
                    Key realKey = null;
                    byte[] objectBytes = null;

                    KeyHelper.validateKey(nextKey);
                    realKey = Key.valueOf(nextKey);
                    objectBytes = getObjectBytes(realKey);

                    Map<String, Object> object = null;

                    if (objectBytes != null) {
                        object = (Map<String, Object>) EncodingHelper
                                .parseSmileLzf(objectBytes);
                        object.remove("id");
                        object.remove("key");
                        object = schemaUntransform(definition, object);
                    } else {
                        object = new LinkedHashMap<String, Object>();
                        object.put("$deleted", true);
                    }

                    Map<String, Object> result = new LinkedHashMap<String, Object>();
                    result.put("id", realKey.getEncryptedIdentifier());
                    result.put("kind", realKey.getType());

                    if (type.equals("$schema")) {
                        String typeName = getTypeName(realKey.getId()
                                .intValue());
                        if (typeName != null) {
                            result.put("$type", typeName);
                        }
                    }

                    result.putAll(object);

                    nextKey = advance();

                    return result;
                } catch (Exception e) {
                    if (!(e instanceof RuntimeException)) {
                        throw new RuntimeException(e);
                    } else {
                        throw (RuntimeException) e;
                    }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Response exportAll() throws Exception {
        StringBuilder json = new StringBuilder();
        for (String type : this.getTypes()) {
            SchemaDefinition schema = loadOrCreateEmptySchemaOutsideTxn(getTypeId(type));

            Iterator<Map<String, Object>> entities = this
                    .iterator(type, schema);

            while (entities.hasNext()) {
                Map<String, Object> entity = entities.next();
                json.append(EncodingHelper.convertToJson(entity));
                json.append("\n");
            }
        }

        return Response.status(Status.OK).entity(json.toString()).build();
    }

    private List<String> getTypes() {
        return database.inTransaction(new TransactionCallback<List<String>>() {
            @Override
            public List<String> inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                List<String> results = new ArrayList<String>();

                Query<Map<String, Object>> select = handle
                        .createQuery(getPrefix() + "get_entity_types");

                for (Map<String, Object> object : select.list()) {
                    results.add((String) object.get("_type_name"));
                }

                return results;
            }
        });
    }

    private byte[] getObjectBytes(final Key key) throws Exception {
        final String cacheKey = EncodingHelper
                .toKVCacheKey(key.getIdentifier());
        byte[] valueBytesLzf = cache.get(cacheKey);

        if (valueBytesLzf != null) {
            return valueBytesLzf;
        }

        return database.inTransaction(new TransactionCallback<byte[]>() {
            @Override
            public byte[] inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                final int typeId = counters.getTypeId(key.getType());
                final long keyId = key.getId();

                Query<Map<String, Object>> select = handle
                        .createQuery(getPrefix() + "retrieve");

                select.bind("key_type", typeId);
                select.bind("key_id", keyId);

                List<Map<String, Object>> results = select.list();

                if (results == null || results.isEmpty()) {
                    return null;
                }

                byte[] valueBytesLzf = (byte[]) results.iterator().next()
                        .get("_value");

                cache.put(EncodingHelper.toKVCacheKey(key.getIdentifier()),
                        valueBytesLzf);

                return valueBytesLzf;
            }
        });
    }

    private Iterator<String> createKeyIterator(final String type) {
        return database.withHandle(new HandleCallback<Iterator<String>>() {
            @Override
            public Iterator<String> withHandle(Handle handle) throws Exception {
                final int typeId = counters.getTypeId(type);

                Query<Map<String, Object>> select = handle
                        .createQuery(getPrefix() + "key_ids_of_type");
                select.bind("key_type", typeId);

                List<String> inefficentButExpedient = new ArrayList<String>();

                Iterator<Map<String, Object>> iter = select.iterator();
                while (iter.hasNext()) {
                    Map<String, Object> next = iter.next();
                    inefficentButExpedient.add(Key.valueOf(
                            type + ":" + next.get("_key_id"))
                            .getEncryptedIdentifier());
                }

                return inefficentButExpedient.iterator();
            }
        });
    }

    private void performInitialization(Handle handle) {
        handle.createStatement(getPrefix() + "init_key_types").execute();
        handle.createStatement(getPrefix() + "init_sequences").execute();
        handle.createStatement(getPrefix() + "init_key_values").execute();
        handle.createStatement(getPrefix() + "init_key_values_index").execute();

        handle.createStatement(getPrefix() + "populate_key_types").execute();
        handle.createStatement(getPrefix() + "populate_sequences").execute();
    }

    private Response makeRetrieveResponse(final String type, final String key,
            byte[] valueBytesLzf, SchemaDefinition definition) throws Exception {
        Map<String, Object> found = (LinkedHashMap<String, Object>) EncodingHelper
                .parseSmileLzf(valueBytesLzf);
        found.remove("id");
        found.remove("kind");

        found = schemaUntransform(definition, found);

        LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("id", Key.valueOf(key).getEncryptedIdentifier());
        value.put("kind", type);
        value.putAll(found);

        String valueJson = EncodingHelper.convertToJson(value);
        Response createResponse = Response.status(Status.OK).entity(valueJson)
                .build();

        return createResponse;
    }

    private Response makeMultiRetrieveResponse(final List<String> keys,
            final Map<String, byte[]> cacheFound, Map<String, Object> dbFound,
            Map<String, SchemaDefinition> cachedSchema) throws Exception {
        Map<String, Object> result = new LinkedHashMap<String, Object>();

        for (String key : keys) {
            Key realKey = Key.valueOf(key);
            SchemaDefinition definition = cachedSchema.get(realKey.getType());

            if (cacheFound.containsKey(key)) {
                Map<String, Object> cached = (Map<String, Object>) EncodingHelper
                        .parseSmileLzf(cacheFound.get(key));
                cached.remove("id");
                cached.remove("kind");

                cached = schemaUntransform(definition, cached);

                Map<String, Object> newResult = new LinkedHashMap<String, Object>();
                newResult.put("id", realKey.getEncryptedIdentifier());
                newResult.put("kind", realKey.getType());
                newResult.putAll(cached);

                result.put(key, cached);
            } else if (dbFound.containsKey(key) && dbFound.get(key) != null) {
                result.put(
                        key,
                        schemaUntransform(definition,
                                (Map<String, Object>) dbFound.get(key)));
            } else {
                result.put(key, null);
            }
        }

        String valueJson = EncodingHelper.convertToJson(result);

        Response multiResponse = Response.status(Status.OK).entity(valueJson)
                .build();
        return multiResponse;
    }

    private SchemaDefinition loadOrCreateEmptySchema(Handle handle,
            final int typeId) throws Exception {
        if (typeId == 1) {
            return SchemaHelper.getEmptySchema();
        }

        SchemaDefinition definition = doLoadSchema(typeId);

        if (definition == null) {
            definition = SchemaHelper.getEmptySchema();

            int inserted = doInsert(handle, 1, typeId,
                    EncodingHelper.convertToSmileLzf(SchemaHelper
                            .getEmptySchema()));

            if (inserted != 1) {
                throw new WebApplicationException(Response
                        .status(Status.INTERNAL_SERVER_ERROR)
                        .entity("Unable to create schema").build());
            }
        }

        if (definition == null) {
            throw new WebApplicationException(Response
                    .status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Unable to create schema").build());
        }

        return definition;
    }

    private SchemaDefinition loadOrCreateEmptySchemaOutsideTxn(final int typeId)
            throws Exception {
        if (typeId == 1) {
            return SchemaHelper.getEmptySchema();
        }

        SchemaDefinition definition = doLoadSchema(typeId);

        if (definition == null) {
            final SchemaDefinition newDefinition = SchemaHelper
                    .getEmptySchema();
            final byte[] definitionBytes = EncodingHelper
                    .convertToSmileLzf(newDefinition);

            try {
                int inserted = database
                        .withHandle(new HandleCallback<Integer>() {
                            @Override
                            public Integer withHandle(Handle handle)
                                    throws Exception {
                                return doInsert(handle, 1, typeId,
                                        definitionBytes);
                            }
                        });

                if (inserted != 1) {
                    throw new WebApplicationException(Response
                            .status(Status.INTERNAL_SERVER_ERROR)
                            .entity("Unable to create schema").build());
                }

                cache.put(EncodingHelper.toKVCacheKey("$schema:" + typeId),
                        definitionBytes);
            } catch (Exception e) {
                e.printStackTrace();
            }

            definition = doLoadSchema(typeId);
        }

        if (definition == null) {
            throw new WebApplicationException(Response
                    .status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Unable to create schema").build());
        }

        return definition;
    }

    private SchemaDefinition doLoadSchema(final int typeId) throws Exception,
            IOException, JsonParseException, JsonMappingException {
        SchemaDefinition definition = null;

        Response schemaResponse = JDBIKeyValueStorage.this.retrieve("$schema:"
                + typeId);

        if (schemaResponse.getStatus() == 200) {
            definition = mapper.readValue(
                    schemaResponse.getEntity().toString(),
                    SchemaDefinition.class);
        }

        return definition;
    }

    private Map<String, Object> applySchema(SchemaDefinition definition,
            final Map<String, Object> readValue) throws Exception {
        try {
            SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                    definition);

            return transformer.validateTransform(readValue);
        } catch (ValidationException e) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity(e.getMessage()).build());
        }
    }

    private Map<String, Object> schemaUntransform(SchemaDefinition definition,
            Map<String, Object> found) throws Exception {
        SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                definition);

        return transformer.untransform((Map<String, Object>) found);
    }

    private int doInsert(Handle handle, final int typeId, final long nextId,
            byte[] valueBytes) {
        Update update = handle.createStatement(getPrefix() + "create");
        update.bind("key_type", typeId);
        update.bind("key_id", nextId);
        update.bind("created_dt", getEpochSecondsNow());
        update.bind("value", valueBytes);
        int inserted = update.execute();

        return inserted;
    }

    private int doUpdate(Handle handle, final int typeId, final long keyId,
            byte[] valueBytes) {
        Update update = handle.createStatement(getPrefix() + "update");
        update.bind("key_type", typeId);
        update.bind("key_id", keyId);
        update.bind("updated_dt", getEpochSecondsNow());
        update.bind("value", valueBytes);
        int updated = update.execute();

        return updated;
    }

    private int getEpochSecondsNow() {
        return (int) (new DateTime().withZone(DateTimeZone.UTC).getMillis() / 1000);
    }

    private byte[] getStorableSmileLzf(Map<String, Object> toInsert) {
        toInsert.remove("id");
        toInsert.remove("kind");

        byte[] valueBytes = EncodingHelper.convertToSmileLzf(toInsert);
        return valueBytes;
    }

    private String getDisplayJson(Key key, final Map<String, Object> entity)
            throws Exception {
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("id", key.getEncryptedIdentifier());
        value.put("kind", key.getType());
        value.putAll(entity);

        String valueJson = EncodingHelper.convertToJson(value);
        return valueJson;
    }

    private static Response getErrorResponse(Exception e) {
        if (e instanceof WebApplicationException) {
            return ((WebApplicationException) e).getResponse();
        } else if (e.getCause() instanceof WebApplicationException) {
            return ((WebApplicationException) e.getCause()).getResponse();
        } else if (e instanceof IllegalArgumentException) {
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
                    .build();
        } else {
            e.printStackTrace();

            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).build();
        }
    }
}
