package com.g414.st9.proto.service.store;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

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
import com.g414.st9.proto.service.count.JDBICountService;
import com.g414.st9.proto.service.helper.AvailabilityManager;
import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.schema.CounterDefinition;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaHelper;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.sequence.SequenceService;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Abstract implementation of key-value storage based on JDBI.
 */
public abstract class JDBIKeyValueStorage implements KeyValueStorage,
        LifecycleRegistration {
    public static int MULTIGET_MAX_KEYS = 100;

    @Inject
    protected AvailabilityManager availability;

    @Inject
    protected IDBI database;

    @Inject
    protected KeyValueCache cache;

    @Inject
    protected JDBISecondaryIndex index;

    @Inject
    protected JDBICountService counts;

    @Inject
    protected SequenceService sequences;

    @Inject
    @Named("nuke.allowed")
    protected Boolean allowNuke;

    protected abstract String getPrefix();

    protected final ObjectMapper mapper = new ObjectMapper();

    protected final Lock nukeLock = new ReentrantLock();

    @Inject
    public void register(Lifecycle lifecycle) {
        lifecycle.register(new LifecycleSupportBase() {
            @Override
            public void init() {
                JDBIKeyValueStorage.this.initialize();

                availability.setAvailable(true);
            }

            @Override
            public void shutdown() {
                sequences.getCurrentCounters();

                availability.setAvailable(false);
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

        sequences.initializeCountersFromDb();
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#create(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response create(final String type, String inValue, final Long id,
            final Long version, boolean strictType) throws Exception {
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
        readValue.remove("version");

        final SchemaDefinition definition;
        final Key newKey;

        try {
            definition = loadOrCreateEmptySchemaOutsideTxn(sequences.getTypeId(
                    type, false));
            if (id != null) {
                newKey = new Key(type, id);
                sequences.bumpKey(type, id);
            } else {
                newKey = sequences.nextKey(type);
            }
        } catch (Exception e) {
            return getErrorResponse(e);
        }

        final Long realVersion = (version != null) ? version : 1L;
        final String valueJson = getDisplayJson(newKey, realVersion, readValue);

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    Map<String, Object> toInsert = applySchema(definition,
                            readValue);

                    for (IndexDefinition indexDef : definition.getIndexes()) {
                        index.insertEntity(handle, newKey.getId(), toInsert,
                                type, indexDef.getName(), definition);
                    }

                    byte[] storeValueBytes = getStorableSmileLzf(toInsert);

                    Map<String, Object> cacheInsert = new LinkedHashMap<String, Object>();
                    cacheInsert.put("version", realVersion.toString());
                    cacheInsert.putAll(toInsert);

                    byte[] cacheValueBytes = EncodingHelper
                            .convertToSmileLzf(cacheInsert);

                    int inserted = doInsert(handle,
                            sequences.getTypeId(type, false), newKey.getId(),
                            realVersion.longValue(), storeValueBytes);

                    if (inserted > 0) {
                        cache.put(EncodingHelper.toKVCacheKey(newKey
                                .getIdentifier()), cacheValueBytes);

                        for (CounterDefinition counterDef : definition
                                .getCounters()) {
                            counts.insertEntity(handle, newKey.getId(),
                                    toInsert, type, counterDef.getName(),
                                    definition);
                        }
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
     * @see com.g414.st9.proto.service.store.KeyValueStorage#createDeleted(String,
     *      Long)
     */
    @Override
    public Response createDeleted(final String type, final Long id)
            throws Exception {
        if (type == null || ((type.contains("@") || type.contains("$")))) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'type'").build();
        }

        if (id == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'id'").build();
        }

        try {
            sequences.bumpKey(type, id);
        } catch (Exception e) {
            return getErrorResponse(e);
        }

        Response insert = database
                .inTransaction(new TransactionCallback<Response>() {
                    @Override
                    public Response inTransaction(Handle handle,
                            TransactionStatus status) throws Exception {
                        int result = doInsert(handle,
                                sequences.getTypeId(type, false).intValue(),
                                id, 1L, EncodingHelper
                                        .convertToSmileLzf(Collections
                                                .emptyMap()));

                        if (result == 1) {
                            return null;
                        } else {
                            return Response
                                    .status(Status.INTERNAL_SERVER_ERROR)
                                    .entity("Unexpected number of rows inserted: "
                                            + result).build();
                        }
                    }
                });

        if (insert != null) {
            return insert;
        }

        Response delete = this.delete(
                new Key(type, id).getEncryptedIdentifier(), false);

        if (delete.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
            return Response
                    .status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Unexpected delete result: (" + delete.getStatus()
                            + ") " + delete.getEntity()).build();
        }

        return Response
                .status(Status.OK)
                .entity(EncodingHelper.convertToJson(ImmutableMap
                        .<String, Object> of("id",
                                new Key(type, id).getEncryptedIdentifier(),
                                "kind", type, "$deleted", true))).build();
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#retrieve(java.lang.String)
     */
    @Override
    public Response retrieve(final String key, final Boolean includeQuarantine)
            throws Exception {
        boolean doQuarantine = includeQuarantine != null && includeQuarantine;

        try {
            KeyHelper.validateKey(key);
            Key realKey = Key.valueOf(key);

            byte[] objectBytes = getObjectBytes(realKey, true, doQuarantine);
            if (objectBytes == null) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            return makeRetrieveResponse((String) realKey.getType(), key,
                    objectBytes,
                    loadOrCreateEmptySchemaOutsideTxn(sequences.getTypeId(
                            realKey.getType(), false)));
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
    public Response multiRetrieve(final List<String> keys,
            final Boolean includeQuarantine) throws Exception {
        final boolean doQuarantine = includeQuarantine != null
                && includeQuarantine;

        if (keys == null || keys.isEmpty()) {
            return Response
                    .status(Status.OK)
                    .entity(EncodingHelper.convertToJson(Collections
                            .<String, Object> emptyMap())).build();
        }

        if (keys.size() > MULTIGET_MAX_KEYS) {
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
                            loadOrCreateEmptySchemaOutsideTxn(sequences
                                    .getTypeId(realKey.getType(), false)));
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
                    Collections.<String, Object> emptyMap(), schemaDefinitions);
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

                        final int typeId = sequences.getTypeId(
                                realKey.getType(), false);
                        final long keyId = realKey.getId();

                        String selectStmt = getPrefix()
                                + (doQuarantine ? "retrieve_quarantined"
                                        : "retrieve");

                        Query<Map<String, Object>> select = handle
                                .createQuery(selectStmt);

                        select.bind("key_type", typeId);
                        select.bind("key_id", keyId);

                        List<Map<String, Object>> results = select.list();

                        if (results == null || results.isEmpty()) {
                            dbFound.put(key, null);

                            continue;
                        }

                        Map<String, Object> first = results.iterator().next();
                        Long version = ((Number) first.get("_version"))
                                .longValue();

                        byte[] valueBytesLzf = (byte[]) first.get("_value");
                        LinkedHashMap<String, Object> found = (LinkedHashMap<String, Object>) EncodingHelper
                                .parseSmileLzf(valueBytesLzf);
                        found.remove("id");
                        found.remove("kind");
                        found.remove("version");

                        LinkedHashMap<String, Object> cacheIn = new LinkedHashMap<String, Object>();
                        cacheIn.put("version", version.toString());
                        cacheIn.putAll(found);

                        LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
                        value.put("id", Key.valueOf(key)
                                .getEncryptedIdentifier());
                        value.put("kind", realKey.getType());
                        value.put("version", version.toString());
                        value.putAll(found);

                        dbFound.put(key, value);

                        cache.put(EncodingHelper.toKVCacheKey(realKey
                                .getIdentifier()), EncodingHelper
                                .convertToSmileLzf(cacheIn));
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

        final Object versionObject = readValue.remove("version");
        if (versionObject == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("missing 'version'").build();
        }

        final SchemaDefinition definition = loadOrCreateEmptySchemaOutsideTxn(sequences
                .getTypeId(realKey.getType(), false));

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    final int typeId = sequences.getTypeId(realKey.getType(),
                            false);
                    final long keyId = realKey.getId();

                    String oldVersionString = null;
                    Map<String, Object> original = null;

                    if (!definition.getCounters().isEmpty()) {
                        original = (Map<String, Object>) EncodingHelper
                                .parseSmileLzf(getObjectBytes(realKey, false));

                        if (original != null) {
                            oldVersionString = (String) original.get("version");
                        } else {
                            return Response.status(Status.NOT_FOUND).entity("")
                                    .build();
                        }
                    } else {
                        oldVersionString = getOldVersion(handle, typeId, keyId);

                        if (oldVersionString == null) {
                            return Response.status(Status.NOT_FOUND).entity("")
                                    .build();
                        }
                    }

                    if (!versionObject.toString().equals(oldVersionString)) {
                        return Response.status(Status.CONFLICT)
                                .entity("version conflict").build();
                    }

                    Long oldVersion = Long.parseLong(oldVersionString);
                    Long newVersion = oldVersion + 1L;

                    String valueJson = getDisplayJson(realKey, newVersion,
                            readValue);

                    Map<String, Object> toUpdate = applySchema(definition,
                            readValue);

                    for (IndexDefinition indexDef : definition.getIndexes()) {
                        index.updateEntity(handle, Long.valueOf(keyId),
                                toUpdate, realKey.getType(),
                                indexDef.getName(), definition);
                    }

                    byte[] storeValueBytes = getStorableSmileLzf(toUpdate);

                    Map<String, Object> cacheUpdate = new LinkedHashMap<String, Object>();
                    cacheUpdate.put("version", newVersion.toString());
                    cacheUpdate.putAll(toUpdate);

                    byte[] cacheValueBytes = EncodingHelper
                            .convertToSmileLzf(cacheUpdate);

                    int updated = doUpdate(handle, typeId, keyId, oldVersion,
                            storeValueBytes);

                    if (updated > 0) {
                        cache.put(EncodingHelper.toKVCacheKey(realKey
                                .getIdentifier()), cacheValueBytes);

                        for (CounterDefinition counterDef : definition
                                .getCounters()) {
                            counts.updateEntity(handle, keyId, original,
                                    toUpdate, realKey.getType(),
                                    counterDef.getName(), definition);
                        }
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

            private String getOldVersion(Handle handle, final int typeId,
                    final long keyId) {
                Query<Map<String, Object>> select = handle
                        .createQuery(getPrefix() + "get_version");

                select.bind("key_type", typeId);
                select.bind("key_id", keyId);

                List<Map<String, Object>> results = select.list();

                if (results == null || results.isEmpty()) {
                    return null;
                }

                return results.get(0).get("_version").toString();
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#delete(java.lang.String)
     */
    @Override
    public Response delete(final String key,
            final boolean updateIndexAndCounters) throws Exception {
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
                    final int typeId = sequences.getTypeId(realKey.getType(),
                            false);
                    final long keyId = realKey.getId();

                    SchemaDefinition definition = loadOrCreateEmptySchema(
                            handle, typeId);

                    Map<String, Object> original = null;

                    if (!definition.getCounters().isEmpty()) {
                        original = (Map<String, Object>) EncodingHelper
                                .parseSmileLzf(getObjectBytes(realKey, false,
                                        true));

                        if (original == null) {
                            return Response.status(Status.NOT_FOUND).entity("")
                                    .build();
                        }
                    }

                    if (updateIndexAndCounters) {
                        for (IndexDefinition indexDef : definition.getIndexes()) {
                            index.deleteEntity(handle, Long.valueOf(keyId),
                                    realKey.getType(), indexDef.getName());
                        }
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

                        if (updateIndexAndCounters) {
                            for (CounterDefinition counterDef : definition
                                    .getCounters()) {
                                counts.deleteEntity(handle, keyId,
                                        realKey.getType(), original,
                                        counterDef.getName(), definition);
                            }
                        }
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
     * @see com.g414.st9.proto.service.store.KeyValueStorage#quarantine(java.lang.String,
     *      boolean)
     */
    @Override
    public Response setQuarantined(final String key, final boolean isQuarantined)
            throws Exception {
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
                    final int typeId = sequences.getTypeId(realKey.getType(),
                            false);
                    final long keyId = realKey.getId();

                    SchemaDefinition definition = loadOrCreateEmptySchema(
                            handle, typeId);

                    Map<String, Object> original = null;

                    if (!definition.getCounters().isEmpty()) {
                        byte[] originalBytes = getObjectBytes(realKey, false,
                                true);

                        if (originalBytes == null) {
                            return Response.status(Status.NOT_FOUND).entity("")
                                    .build();
                        }

                        original = (Map<String, Object>) EncodingHelper
                                .parseSmileLzf(originalBytes);
                    }

                    Update quarantine = handle.createStatement(getPrefix()
                            + "quarantine");
                    quarantine.bind("is_deleted", isQuarantined ? "Q" : "N");
                    quarantine.bind("updated_dt", getEpochSecondsNow());
                    quarantine.bind("key_type", typeId);
                    quarantine.bind("key_id", keyId);
                    quarantine.bind("cur_deleted", !isQuarantined ? "Q" : "N");

                    int quarantined = quarantine.execute();

                    if (quarantined > 0) {
                        cache.delete(EncodingHelper.toKVCacheKey(realKey
                                .getIdentifier()));

                        for (CounterDefinition counterDef : definition
                                .getCounters()) {
                            if (isQuarantined) {
                                counts.deleteEntity(handle, keyId,
                                        realKey.getType(), original,
                                        counterDef.getName(), definition);
                            } else {
                                counts.insertEntity(handle,
                                        Long.valueOf(keyId), original,
                                        realKey.getType(),
                                        counterDef.getName(), definition);
                            }
                        }

                        for (IndexDefinition indexDef : definition.getIndexes()) {
                            index.setEntityQuarantine(handle,
                                    Long.valueOf(keyId), realKey.getType(),
                                    indexDef.getName(), isQuarantined);
                        }
                    }

                    return (quarantined == 0) ? Response
                            .status(Status.NOT_FOUND).entity("").build()
                            : Response.status(Status.NO_CONTENT).entity("")
                                    .build();
                } catch (WebApplicationException e) {
                    e.printStackTrace();
                    return e.getResponse();
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                    return getErrorResponse(e);
                }
            }
        });
    }

    @Override
    public Response clearRequested(boolean preserveSchema) throws Exception {
        if (!allowNuke) {
            return Response.status(Status.FORBIDDEN)
                    .entity("'nuke' operation not allowed").build();
        }

        clear(preserveSchema);

        return Response.status(Status.NO_CONTENT).entity("").build();
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#clear()
     */
    @Override
    public void clear(final boolean preserveSchema) {
        nukeLock.lock();

        try {
            database.inTransaction(new TransactionCallback<Void>() {
                @Override
                public Void inTransaction(Handle handle,
                        TransactionStatus status) throws Exception {
                    try {
                        index.clear(handle, iterator("$schema"), preserveSchema);
                        counts.clear(handle, iterator("$schema"),
                                preserveSchema);

                        if (preserveSchema) {
                            handle.createStatement(
                                    getPrefix() + "reset_sequences").execute();
                            handle.createStatement(
                                    getPrefix() + "reset_key_values").execute();
                        } else {
                            handle.createStatement(
                                    getPrefix() + "truncate_key_types")
                                    .execute();
                            handle.createStatement(
                                    getPrefix() + "truncate_sequences")
                                    .execute();
                            handle.createStatement(
                                    getPrefix() + "truncate_key_values")
                                    .execute();

                            performInitialization(handle);
                        }

                        return null;
                    } catch (Exception e) {
                        e.printStackTrace();

                        throw new RuntimeException(e);
                    }
                }
            });

            cache.clear();
            sequences.clear();
        } finally {
            nukeLock.unlock();
        }
    }

    @Override
    public Iterator<Map<String, Object>> iterator(String type) throws Exception {
        return iterator(type,
                loadOrCreateEmptySchemaOutsideTxn(sequences.getTypeId(type,
                        false)));
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
                    objectBytes = getObjectBytes(realKey, false, true);

                    Map<String, Object> object = null;

                    if (objectBytes != null) {
                        try {
                            object = (Map<String, Object>) EncodingHelper
                                    .parseSmileLzf(objectBytes);
                        } catch (Exception e) {
                            e.printStackTrace();
                            object = new LinkedHashMap<String, Object>();
                            object.put("$error", true);
                        }
                        object.remove("id");
                        object.remove("kind");
                        object = schemaUntransform(definition, object);
                    } else {
                        object = new LinkedHashMap<String, Object>();
                        object.put("$deleted", true);
                    }

                    Map<String, Object> result = new LinkedHashMap<String, Object>();
                    result.put("id", realKey.getEncryptedIdentifier());
                    result.put("kind", realKey.getType());

                    if (type.equals("$schema")) {
                        String typeName = sequences.getTypeName(realKey.getId()
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
        return Response.status(Status.OK).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException,
                    WebApplicationException {
                PrintWriter output = new PrintWriter(new OutputStreamWriter(
                        out, "UTF-8"));
                output.println("{\"$export\":\"BEGIN\"}");

                try {
                    for (String type : JDBIKeyValueStorage.this.getTypes()) {
                        SchemaDefinition schema = loadOrCreateEmptySchemaOutsideTxn(sequences
                                .getTypeId(type, false));

                        Iterator<Map<String, Object>> entities = JDBIKeyValueStorage.this
                                .iterator(type, schema);
                        while (entities.hasNext()) {
                            Map<String, Object> entity = entities.next();
                            output.println(EncodingHelper.convertToJson(entity));
                        }
                    }

                    output.println("{\"$export\":\"OK\"}");
                } catch (Exception e) {
                    e.printStackTrace();
                    output.println("{\"$export\":\"ERROR\"}");
                } finally {
                    output.close();
                }
            }
        }).build();
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

    private byte[] getObjectBytes(final Key key, boolean cacheOk)
            throws Exception {
        return getObjectBytes(key, cacheOk, false);
    }

    private byte[] getObjectBytes(final Key key, final boolean cacheOk,
            final boolean quarantineOk) throws Exception {
        if (cacheOk) {
            final String cacheKey = EncodingHelper.toKVCacheKey(key
                    .getIdentifier());
            byte[] valueBytesLzf = cache.get(cacheKey);

            if (valueBytesLzf != null) {
                return valueBytesLzf;
            }
        }

        return database.inTransaction(new TransactionCallback<byte[]>() {
            @Override
            public byte[] inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                final int typeId = sequences.getTypeId(key.getType(), false);
                final long keyId = key.getId();

                String queryName = getPrefix()
                        + (quarantineOk ? "retrieve_quarantined" : "retrieve");
                Query<Map<String, Object>> select = handle
                        .createQuery(queryName);

                select.bind("key_type", typeId);
                select.bind("key_id", keyId);

                List<Map<String, Object>> results = select.list();

                if (results == null || results.isEmpty()) {
                    return null;
                }

                Map<String, Object> first = results.iterator().next();

                String rowStatus = (String) first.get("_is_deleted");
                Long version = ((Number) first.get("_version")).longValue();
                byte[] foundBytes = (byte[]) first.get("_value");

                Map<String, Object> found = (Map<String, Object>) EncodingHelper
                        .parseSmileLzf(foundBytes);

                Map<String, Object> withVersion = new LinkedHashMap<String, Object>();

                if (quarantineOk) {
                    withVersion.put("$status", rowStatus);
                    if ("Y".equals(rowStatus)) {
                        withVersion.put("$deleted", true);
                    } else if ("Q".equals(rowStatus)) {
                        withVersion.put("$quarantined", true);
                    } else {
                        // no marker field
                    }
                }

                withVersion.put("version", version.toString());
                withVersion.putAll(found);

                byte[] valueBytesLzf = EncodingHelper
                        .convertToSmileLzf(withVersion);

                if (!quarantineOk) {
                    cache.put(EncodingHelper.toKVCacheKey(key.getIdentifier()),
                            valueBytesLzf);
                }

                return valueBytesLzf;
            }
        });
    }

    private Iterator<String> createKeyIterator(final String type) {
        return database.withHandle(new HandleCallback<Iterator<String>>() {
            @Override
            public Iterator<String> withHandle(Handle handle) throws Exception {
                final int typeId = sequences.getTypeId(type, false);

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

        try {
            handle.createStatement(getPrefix() + "populate_key_types")
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            handle.createStatement(getPrefix() + "populate_sequences")
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

                result.put(key, newResult);
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

            int inserted = doInsert(handle, 1, typeId, 1L,
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

            final Map<String, Object> newDefinitionWithVersion = new LinkedHashMap<String, Object>();
            newDefinitionWithVersion.put("version", "1");
            newDefinitionWithVersion.putAll(mapper.readValue(
                    mapper.writeValueAsString(newDefinition),
                    LinkedHashMap.class));

            try {
                int inserted = database
                        .withHandle(new HandleCallback<Integer>() {
                            @Override
                            public Integer withHandle(Handle handle)
                                    throws Exception {
                                return doInsert(
                                        handle,
                                        1,
                                        typeId,
                                        1L,
                                        EncodingHelper
                                                .convertToSmileLzf(newDefinition));
                            }
                        });

                if (inserted != 1) {
                    throw new WebApplicationException(Response
                            .status(Status.INTERNAL_SERVER_ERROR)
                            .entity("Unable to create schema").build());
                }

                cache.put(EncodingHelper.toKVCacheKey("$schema:" + typeId),
                        EncodingHelper
                                .convertToSmileLzf(newDefinitionWithVersion));
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
                + typeId, false);

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
            final long version, byte[] valueBytes) {
        Update update = handle.createStatement(getPrefix() + "create");
        update.bind("key_type", typeId);
        update.bind("key_id", nextId);
        update.bind("created_dt", getEpochSecondsNow());
        update.bind("version", version);
        update.bind("value", valueBytes);
        int inserted = update.execute();

        return inserted;
    }

    private int doUpdate(Handle handle, final int typeId, final long keyId,
            final long oldVersion, byte[] valueBytes) {
        Update update = handle.createStatement(getPrefix() + "update");
        update.bind("key_type", typeId);
        update.bind("key_id", keyId);
        update.bind("updated_dt", getEpochSecondsNow());
        update.bind("old_version", oldVersion);
        update.bind("new_version", oldVersion + 1L);
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
        toInsert.remove("version");

        byte[] valueBytes = EncodingHelper.convertToSmileLzf(toInsert);
        return valueBytes;
    }

    private String getDisplayJson(Key key, Long version,
            final Map<String, Object> entity) throws Exception {
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("id", key.getEncryptedIdentifier());
        value.put("kind", key.getType());
        value.put("version", version.toString());
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
