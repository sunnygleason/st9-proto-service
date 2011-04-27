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

import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaHelper;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;

/**
 * In-memory implementation of key-value storage.
 */
public class InMemoryKeyValueStorage implements KeyValueStorage {
    private final Map<String, byte[]> storage = Collections
            .synchronizedMap(new LinkedHashMap<String, byte[]>());
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
    public Response create(String type, String inValue, Long id, Long version,
            boolean strictType) throws Exception {
        if (type == null
                || (strictType && ((type.contains("@") || type.contains("$"))))) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'type'").build();
        }

        try {
            validateType(type);
            Map<String, Object> readValue = EncodingHelper
                    .parseJsonString(inValue);
            readValue.remove("id");
            readValue.remove("kind");
            readValue.put("version", "1");

            Long theId = this.nextId(type, id);

            Key key = new Key(type, theId);
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", key.getEncryptedIdentifier());
            value.put("kind", type);
            value.putAll(readValue);

            String valueJson = EncodingHelper.convertToJson(value);

            value.remove("id");
            value.remove("kind");

            byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

            storage.put(key.getIdentifier(), valueBytes);

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
            KeyHelper.validateKey(key);
            Key realKey = Key.valueOf(key);
            byte[] valueBytesLzf = storage.get(realKey.getIdentifier());

            if (valueBytesLzf == null) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            LinkedHashMap<String, Object> readValue = (LinkedHashMap<String, Object>) EncodingHelper
                    .parseSmileLzf(valueBytesLzf);
            readValue.remove("id");
            readValue.remove("kind");

            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", realKey.getEncryptedIdentifier());
            value.put("kind", realKey.getType());
            value.putAll(readValue);

            String jsonValue = EncodingHelper.convertToJson(value);

            return Response.status(Status.OK).entity(jsonValue).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        } catch (IllegalArgumentException e) {
            return getErrorResponse(e);
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
                Key realKey = Key.valueOf(key);
                byte[] valueBytesLzf = storage.get(realKey.getIdentifier());

                if (valueBytesLzf == null) {
                    result.put(key, null);

                    continue;
                }

                LinkedHashMap<String, Object> readValue = (LinkedHashMap<String, Object>) EncodingHelper
                        .parseSmileLzf(valueBytesLzf);
                readValue.remove("id");
                readValue.remove("kind");

                Map<String, Object> value = new LinkedHashMap<String, Object>();
                value.put("id", realKey.getEncryptedIdentifier());
                value.put("kind", realKey.getType());
                value.putAll(readValue);

                result.put(key, value);
            }

            String jsonValue = EncodingHelper.convertToJson(result);

            return Response.status(Status.OK).entity(jsonValue).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        } catch (IllegalArgumentException e) {
            return getErrorResponse(e);
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#update(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Response update(String key, String inValue) throws Exception {
        try {
            Key realKey = Key.valueOf(key);

            Map<String, Object> readValue = EncodingHelper
                    .parseJsonString(inValue);
            readValue.remove("id");
            readValue.remove("kind");

            final Object versionObject = readValue.remove("version");
            if (versionObject == null) {
                return Response.status(Status.BAD_REQUEST)
                        .entity("missing 'version'").build();
            }

            String inVersion = versionObject.toString();

            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put("id", realKey.getIdentifier());
            value.put("kind", realKey.getType());
            value.put("version", Long.toString(Long.parseLong(versionObject
                    .toString()) + 1L));
            value.putAll(readValue);

            if (!storage.containsKey(key)) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            Map<String, Object> oldObject = (Map<String, Object>) EncodingHelper
                    .parseSmileLzf(storage.get(realKey.getIdentifier()));
            String oldVersion = (String) oldObject.get("version");

            if (oldVersion == null || !oldVersion.equals(inVersion)) {
                return Response.status(Status.CONFLICT)
                        .entity("version conflict").build();
            }

            String valueJson = EncodingHelper.convertToJson(value);

            value.remove("id");
            value.remove("kind");

            byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

            storage.put(key, valueBytes);

            return Response.status(Status.OK).entity(valueJson).build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        } catch (IllegalArgumentException e) {
            return getErrorResponse(e);
        }
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#delete(java.lang.String)
     */
    @Override
    public Response delete(String key) throws Exception {
        try {
            KeyHelper.validateKey(key);
            Key realKey = Key.valueOf(key);

            if (!storage.containsKey(realKey.getIdentifier())) {
                return Response.status(Status.NOT_FOUND).entity("").build();
            }

            storage.remove(realKey.getIdentifier());

            return Response.status(Status.NO_CONTENT).entity("").build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        } catch (IllegalArgumentException e) {
            return getErrorResponse(e);
        }
    }

    @Override
    public Response clearRequested(boolean preserveSchema) throws Exception {
        if (preserveSchema) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("preserve schema not implemented").build();
        }

        clear(preserveSchema);

        return Response.status(Status.NO_CONTENT).entity("").build();
    }

    /**
     * @see com.g414.st9.proto.service.store.KeyValueStorage#clear()
     */
    @Override
    public void clear(boolean preserveSchema) {
        if (preserveSchema) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("preserve schema not implemented").build());
        }

        this.sequences.clear();
        this.storage.clear();
    }

    @Override
    public Iterator<Map<String, Object>> iterator(String type) throws Exception {
        return iterator(type, SchemaHelper.getEmptySchema());
    }

    @Override
    public Iterator<Map<String, Object>> iterator(final String type,
            SchemaDefinition schemaDefinition) throws Exception {
        return new Iterator<Map<String, Object>>() {
            private final Iterator<String> inner = storage.keySet().iterator();
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

                try {
                    Key realKey = Key.valueOf(nextKey);

                    Map<String, Object> result = new LinkedHashMap<String, Object>();
                    result.put("id", realKey.getEncryptedIdentifier());
                    result.put("kind", realKey.getType());
                    result.putAll(object);

                    nextKey = advance();

                    return result;
                } catch (Exception e) {
                    throw new RuntimeException(e);
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
        for (String type : types.keySet()) {
            Iterator<Map<String, Object>> entities = this.iterator(type, null);

            while (entities.hasNext()) {
                Map<String, Object> entity = entities.next();
                json.append(EncodingHelper.convertToJson(entity));
                json.append("\n");
            }
        }

        return Response.status(Status.OK).entity(json.toString()).build();
    }

    private Long nextId(String type, Long id) {
        validateType(type);

        Integer typeId = getTypeId(type);

        AtomicLong aNewSeq = new AtomicLong(0);
        AtomicLong existing = sequences.putIfAbsent(typeId, aNewSeq);

        if (existing == null) {
            existing = aNewSeq;
        }

        if (id != null) {
            existing.set(id - 1);
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

    private static Response getErrorResponse(IllegalArgumentException e) {
        return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
                .build();
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
