package com.g414.st9.proto.service.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleRegistration;
import com.g414.guice.lifecycle.LifecycleSupportBase;
import com.g414.st9.proto.service.cache.KeyValueCache;
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

    protected final Map<String, Integer> typeCodes = new ConcurrentHashMap<String, Integer>();

    protected abstract String getPrefix();

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

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#create(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response create(final String type, String inValue) throws Exception {
        try {
            final Map<String, Object> readValue = EncodingHelper
                    .parseJsonString(inValue);
            readValue.remove("id");

            return database.inTransaction(new TransactionCallback<Response>() {
                @Override
                public Response inTransaction(Handle handle,
                        TransactionStatus status) throws Exception {
                    try {
                        final int typeId = SequenceHelper.validateType(
                                typeCodes, getPrefix(), handle, type, true);

                        final long nextId = SequenceHelper.getNextId(
                                getPrefix(), handle, typeId, true);

                        String key = type + ":" + nextId;

                        Map<String, Object> value = new LinkedHashMap<String, Object>();
                        value.put("id", key);
                        value.putAll(readValue);

                        String valueJson = EncodingHelper.convertToJson(value);

                        value.remove("id");

                        byte[] valueBytes = EncodingHelper
                                .convertToSmileLzf(value);

                        int epochSeconds = (int) (new DateTime().withZone(
                                DateTimeZone.UTC).getMillis() / 1000);

                        Update update = handle.createStatement(getPrefix()
                                + "create");
                        update.bind("key_type", typeId);
                        update.bind("key_id", nextId);
                        update.bind("created_dt", epochSeconds);
                        update.bind("value", valueBytes);
                        int inserted = update.execute();

                        if (inserted > 0) {
                            cache.put(EncodingHelper.toKVCacheKey(key),
                                    valueBytes);
                        }

                        return (inserted == 1) ? Response.status(Status.OK)
                                .entity(valueJson).build() : Response
                                .status(Status.INTERNAL_SERVER_ERROR)
                                .entity("Entity not inserted").build();
                    } catch (WebApplicationException e) {
                        return e.getResponse();
                    }
                }
            });
        } catch (WebApplicationException e) {
            return e.getResponse();
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#retrieve(java.lang.String)
     */
    @Override
    public Response retrieve(final String key) throws Exception {
        final Object[] keyParts;
        try {
            keyParts = KeyHelper.validateKey(key);
        } catch (WebApplicationException e) {
            return e.getResponse();
        }

        byte[] valueBytesLzf = cache.get(EncodingHelper.toKVCacheKey(key));

        if (valueBytesLzf != null) {
            return makeRetrieveResponse(key, valueBytesLzf);
        }

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    byte[] valueBytesLzf = cache.get(EncodingHelper
                            .toKVCacheKey(key));

                    if (valueBytesLzf == null) {
                        final int typeId = SequenceHelper.validateType(
                                typeCodes, getPrefix(), handle,
                                (String) keyParts[0], false);
                        final long keyId = (Long) keyParts[1];

                        Query<Map<String, Object>> select = handle
                                .createQuery(getPrefix() + "retrieve");

                        select.bind("key_type", typeId);
                        select.bind("key_id", keyId);

                        List<Map<String, Object>> results = select.list();

                        if (results == null || results.isEmpty()) {
                            return Response.status(Status.NOT_FOUND).entity("")
                                    .build();
                        }

                        valueBytesLzf = (byte[]) results.iterator().next()
                                .get("_value");
                    }

                    return makeRetrieveResponse(key, valueBytesLzf);
                } catch (WebApplicationException e) {
                    return e.getResponse();
                }
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#multiRetrieve(List)
     */
    @Override
    public Response multiRetrieve(final List<String> keys) throws Exception {
        if (keys == null || keys.isEmpty()) {
            Response.status(Status.OK)
                    .entity(EncodingHelper.convertToJson(Collections
                            .<String, Object> emptyMap())).build();
        }

        final List<String> cacheKeys = new ArrayList<String>();
        try {
            for (String key : keys) {
                cacheKeys.add(EncodingHelper.toKVCacheKey(key));
                KeyHelper.validateKey(key);
            }
        } catch (WebApplicationException e) {
            return e.getResponse();
        }

        final Map<String, byte[]> maybeFound = cache.multiget(cacheKeys);
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
                    Collections.<String, Object> emptyMap());
        }

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                try {
                    Map<String, Object> dbFound = new HashMap<String, Object>();

                    for (String key : notFound) {
                        Object[] keyParts = KeyHelper.validateKey(key);

                        final int typeId = SequenceHelper.validateType(
                                typeCodes, getPrefix(), handle,
                                (String) keyParts[0], false);
                        final long keyId = (Long) keyParts[1];

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
                        LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
                        value.put("id", key);
                        value.putAll(found);

                        dbFound.put(key, value);
                    }

                    return makeMultiRetrieveResponse(keys, cacheFound, dbFound);
                } catch (WebApplicationException e) {
                    return e.getResponse();
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
        final Object[] keyParts = KeyHelper.validateKey(key);

        final Map<String, Object> readValue = EncodingHelper
                .parseJsonString(inValue);

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                final int typeId = SequenceHelper.validateType(typeCodes,
                        getPrefix(), handle, (String) keyParts[0], false);
                final long keyId = (Long) keyParts[1];

                Map<String, Object> value = new LinkedHashMap<String, Object>();
                value.put("id", key);
                value.putAll(readValue);

                String valueJson = EncodingHelper.convertToJson(value);

                value.remove("id");

                byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

                int epochSeconds = (int) (new DateTime().withZone(
                        DateTimeZone.UTC).getMillis() / 1000);

                Update update = handle.createStatement(getPrefix() + "update");
                update.bind("key_type", typeId);
                update.bind("key_id", keyId);
                update.bind("updated_dt", epochSeconds);
                update.bind("value", valueBytes);
                int updated = update.execute();

                if (updated > 0) {
                    cache.put(EncodingHelper.toKVCacheKey(key), valueBytes);
                }

                return (updated == 0) ? Response.status(Status.NOT_FOUND)
                        .entity("").build() : Response.status(Status.OK)
                        .entity(valueJson).build();
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#delete(java.lang.String)
     */
    @Override
    public Response delete(final String key) throws Exception {
        final Object[] keyParts = KeyHelper.validateKey(key);

        return database.inTransaction(new TransactionCallback<Response>() {
            @Override
            public Response inTransaction(Handle handle,
                    TransactionStatus status) throws Exception {
                final int typeId = SequenceHelper.validateType(typeCodes,
                        getPrefix(), handle, (String) keyParts[0], false);
                final long keyId = (Long) keyParts[1];

                Update delete = handle.createStatement(getPrefix() + "delete");
                delete.bind("key_type", typeId);
                delete.bind("key_id", keyId);

                int deleted = delete.execute();

                if (deleted > 0) {
                    cache.delete(EncodingHelper.toKVCacheKey(key));
                }

                return (deleted == 0) ? Response.status(Status.NOT_FOUND)
                        .entity("").build() : Response
                        .status(Status.NO_CONTENT).entity("").build();
            }
        });
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#clear()
     */
    @Override
    public void clear() {
        typeCodes.clear();
        cache.clear();

        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                handle.createStatement(getPrefix() + "truncate_key_types")
                        .execute();
                handle.createStatement(getPrefix() + "truncate_sequences")
                        .execute();
                handle.createStatement(getPrefix() + "truncate_key_values")
                        .execute();

                performInitialization(handle);

                return null;
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

    private Response makeRetrieveResponse(final String key, byte[] valueBytesLzf)
            throws Exception {
        LinkedHashMap<String, Object> found = (LinkedHashMap<String, Object>) EncodingHelper
                .parseSmileLzf(valueBytesLzf);
        LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("id", key);
        value.putAll(found);

        String valueJson = EncodingHelper.convertToJson(value);
        Response createResponse = Response.status(Status.OK).entity(valueJson)
                .build();
        return createResponse;
    }

    private Response makeMultiRetrieveResponse(final List<String> keys,
            final Map<String, byte[]> cacheFound, Map<String, Object> dbFound)
            throws Exception {
        Map<String, Object> result = new LinkedHashMap<String, Object>();

        for (String key : keys) {
            if (cacheFound.containsKey(key)) {
                result.put(key,
                        EncodingHelper.parseSmileLzf(cacheFound.get(key)));
            } else if (dbFound.containsKey(key)) {
                result.put(key, dbFound.get(key));
            } else {
                result.put(key, null);
            }
        }

        String valueJson = EncodingHelper.convertToJson(result);

        Response multiResponse = Response.status(Status.OK).entity(valueJson)
                .build();
        return multiResponse;
    }
}
