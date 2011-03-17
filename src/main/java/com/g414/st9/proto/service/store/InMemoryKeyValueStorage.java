package com.g414.st9.proto.service.store;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final ConcurrentHashMap<String, Integer> types = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentHashMap<Integer, AtomicLong> sequences = new ConcurrentHashMap<Integer, AtomicLong>();
    private final AtomicInteger typeSeq = new AtomicInteger(0);

    @Override
    public synchronized Integer getTypeId(String type) {
        Integer typeId = types.get(type);
        if (typeId == null) {
            typeId = typeSeq.getAndIncrement();
            types.put(type, typeId);
        }

        return typeId;
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#create(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response create(String type, String inValue, Long id)
            throws Exception {
        try {
            validateType(type);
            Map<String, Object> readValue = EncodingHelper
                    .parseJsonString(inValue);
            readValue.remove("id");
            readValue.remove("kind");

            Long theId = (id != null) ? id : this.nextId(type);

            String key = type + ":" + theId;
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", key);
            value.put("kind", type);
            value.putAll(readValue);

            String valueJson = EncodingHelper.convertToJson(value);

            value.remove("id");
            value.remove("kind");

            byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

            storage.put(key, valueBytes);

            return Response.status(Status.OK).entity(valueJson).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#retrieve(java.lang.String)
     */
    @Override
    public Response retrieve(String key) throws Exception {
        try {
            Object[] keyParts = KeyHelper.validateKey(key);

            byte[] valueBytesLzf = storage.get(key);

            if (valueBytesLzf == null) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            LinkedHashMap<String, Object> readValue = (LinkedHashMap<String, Object>) EncodingHelper
                    .parseSmileLzf(valueBytesLzf);
            readValue.remove("id");
            readValue.remove("kind");

            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", key);
            value.put("kind", keyParts[0]);
            value.putAll(readValue);

            String jsonValue = EncodingHelper.convertToJson(value);

            return Response.status(Status.OK).entity(jsonValue).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#multiRetrieve(java.lang.String)
     */
    @Override
    public Response multiRetrieve(List<String> keys) throws Exception {
        try {
            if (keys == null || keys.isEmpty()) {
                Response.status(Status.OK)
                        .entity(EncodingHelper.convertToJson(Collections
                                .<String, Object> emptyMap())).build();
            }

            LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();

            for (String key : keys) {
                Object[] keyParts = KeyHelper.validateKey(key);

                byte[] valueBytesLzf = storage.get(key);

                if (valueBytesLzf == null) {
                    result.put(key, null);

                    continue;
                }

                LinkedHashMap<String, Object> readValue = (LinkedHashMap<String, Object>) EncodingHelper
                        .parseSmileLzf(valueBytesLzf);
                readValue.remove("id");
                readValue.remove("kind");

                Map<String, Object> value = new LinkedHashMap<String, Object>();
                value.put("id", key);
                value.put("kind", keyParts[0]);
                value.putAll(readValue);

                result.put(key, value);
            }

            String jsonValue = EncodingHelper.convertToJson(result);

            return Response.status(Status.OK).entity(jsonValue).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#update(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response update(String key, String inValue) throws Exception {
        Object[] keyParts = KeyHelper.validateKey(key);

        Map<String, Object> readValue = EncodingHelper.parseJsonString(inValue);
        readValue.remove("id");
        readValue.remove("kind");

        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("id", key);
        value.put("kind", keyParts[0]);
        value.putAll(readValue);

        if (!storage.containsKey(key)) {
            return Response.status(Status.NOT_FOUND).entity("").build();
        }

        String valueJson = EncodingHelper.convertToJson(value);

        value.remove("id");
        value.remove("kind");

        byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

        storage.put(key, valueBytes);

        return Response.status(Status.OK).entity(valueJson).build();
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#delete(java.lang.String)
     */
    @Override
    public Response delete(String key) throws Exception {
        KeyHelper.validateKey(key);

        if (!storage.containsKey(key)) {
            return Response.status(Status.NOT_FOUND).entity("").build();
        }

        storage.remove(key);

        return Response.status(Status.NO_CONTENT).entity("").build();
    }

    @Override
    public Response clearRequested() throws Exception {
        clear();

        return Response.status(Status.NO_CONTENT).entity("").build();
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#clear()
     */
    @Override
    public void clear() {
        this.sequences.clear();
        this.storage.clear();
    }

    @Override
    public Iterator<Map<String, Object>> iterator(final String type)
            throws Exception {
        return new Iterator<Map<String, Object>>() {
            private final Iterator<String> inner = storage.keySet().iterator();
            private String nextKey = advance();

            public String advance() {
                String key = null;

                while (key == null && inner.hasNext()) {
                    String newKey = inner.next();
                    Object[] keyParts = KeyHelper.validateKey(newKey);
                    if (type.equals(keyParts[0])) {
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
                byte[] objectBytes = null;
                while (objectBytes == null && nextKey != null) {
                    objectBytes = storage.get(nextKey);
                    if (objectBytes == null) {
                        nextKey = advance();
                    }
                }

                if (objectBytes == null) {
                    return null;
                }

                Map<String, Object> object = (Map<String, Object>) EncodingHelper
                        .parseSmileLzf(objectBytes);
                object.remove("id");
                object.remove("kind");

                Object[] keyParts = KeyHelper.validateKey(nextKey);

                Map<String, Object> result = new LinkedHashMap<String, Object>();
                result.put("id", nextKey);
                result.put("kind", keyParts[0]);
                result.putAll(object);

                nextKey = advance();

                return result;
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
        for (String type : types.keySet()) {
            Iterator<Map<String, Object>> entities = this.iterator(type);
            while (entities.hasNext()) {
                Map<String, Object> entity = entities.next();
                json.append(EncodingHelper.convertToJson(entity));
                json.append("\n");
            }
        }

        return Response.status(Status.OK).entity(json.toString()).build();
    }

    private Long nextId(String type) {
        validateType(type);

        Integer typeId = getTypeId(type);

        AtomicLong aNewSeq = new AtomicLong(0);
        AtomicLong existing = sequences.putIfAbsent(typeId, aNewSeq);

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

    public static class InMemoryKeyValueStorageModule extends AbstractModule {
        @Override
        public void configure() {
            Binder binder = binder();

            binder.bind(KeyValueStorage.class)
                    .to(InMemoryKeyValueStorage.class).asEagerSingleton();
        }
    }
}
