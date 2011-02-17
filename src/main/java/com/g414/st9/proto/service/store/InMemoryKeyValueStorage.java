package com.g414.st9.proto.service.store;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;

/**
 * In-memory implementation of key-value storage.
 */
public class InMemoryKeyValueStorage implements KeyValueStorage {
    private final ConcurrentHashMap<String, byte[]> storage = new ConcurrentHashMap<String, byte[]>();
    private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<String, AtomicLong>();

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.g414.st9.proto.service.store.KeyValueStorage#create(java.lang.String,
     * java.lang.String)
     */
    @Override
    public Response create(String type, String inValue) throws Exception {
        try {
            validateType(type);
            Map<String, Object> readValue = EncodingHelper
                    .parseJsonString(inValue);
            readValue.remove("id");

            String key = type + ":" + this.nextId(type);
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", key);
            value.putAll(readValue);

            String valueJson = EncodingHelper.convertToJson(value);

            value.remove("id");

            byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

            storage.put(key, valueBytes);

            return Response.status(Status.OK).entity(valueJson).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.g414.st9.proto.service.store.KeyValueStorage#retrieve(java.lang.String
     * )
     */
    @Override
    public Response retrieve(String key) throws Exception {
        try {
            validateKey(key);

            byte[] valueBytesLzf = storage.get(key);

            if (valueBytesLzf == null) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            LinkedHashMap<String, Object> readValue = (LinkedHashMap<String, Object>) EncodingHelper
                    .parseSmileLzf(valueBytesLzf);
            readValue.remove("id");

            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", key);
            value.putAll(readValue);

            String jsonValue = EncodingHelper.convertToJson(value);

            return Response.status(Status.OK).entity(jsonValue).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.g414.st9.proto.service.store.KeyValueStorage#update(java.lang.String,
     * java.lang.String)
     */
    @Override
    public Response update(String key, String inValue) throws Exception {
        validateKey(key);
        Map<String, Object> readValue = EncodingHelper.parseJsonString(inValue);
        readValue.remove("id");

        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("id", key);
        value.putAll(readValue);

        if (!storage.containsKey(key)) {
            return Response.status(Status.NOT_FOUND).entity("").build();
        }

        String valueJson = EncodingHelper.convertToJson(value);

        value.remove("id");

        byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

        storage.put(key, valueBytes);

        return Response.status(Status.OK).entity(valueJson).build();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.g414.st9.proto.service.store.KeyValueStorage#delete(java.lang.String)
     */
    @Override
    public Response delete(String key) throws Exception {
        validateKey(key);

        if (!storage.containsKey(key)) {
            return Response.status(Status.NOT_FOUND).entity("").build();
        }

        storage.remove(key);

        return Response.status(Status.NO_CONTENT).entity("").build();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.g414.st9.proto.service.store.KeyValueStorage#clear()
     */
    @Override
    public void clear() {
        this.sequences.clear();
        this.storage.clear();
    }

    private Long nextId(String type) {
        validateType(type);

        AtomicLong aNewSeq = new AtomicLong(0);
        AtomicLong existing = sequences.putIfAbsent(type, aNewSeq);

        if (existing == null) {
            existing = aNewSeq;
        }

        return existing.incrementAndGet();
    }

    private static void validateType(String type) {
        if (type == null || type.length() == 0 || type.indexOf(":") != -1) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.length() == 0 || key.indexOf(":") == -1) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'id'")
                    .build());
        }
    }

    public static class InMemoryKeyValueStorageModule extends AbstractModule {
        @Override
        public void configure() {
            Binder binder = binder();

            binder.bind(KeyValueStorage.class)
                    .to(InMemoryKeyValueStorage.class).asEagerSingleton();
        }
    }
}
