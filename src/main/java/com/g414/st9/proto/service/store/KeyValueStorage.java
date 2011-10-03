package com.g414.st9.proto.service.store;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import com.g414.st9.proto.service.schema.SchemaDefinition;

public interface KeyValueStorage {
    public abstract Response create(String type, String inValue, Long id,
            Long version, boolean strictType) throws Exception;

    public abstract Response createDeleted(String type, Long id)
            throws Exception;

    public abstract Response retrieve(String key, Boolean includeQuarantine)
            throws Exception;

    public abstract Response multiRetrieve(List<String> keys,
            Boolean includeQuarantine) throws Exception;

    public abstract Response update(String key, String inValue)
            throws Exception;

    public abstract Response delete(String key, boolean updateIndexAndCounters)
            throws Exception;

    public abstract Response setQuarantined(String key, boolean isQuarantined)
            throws Exception;

    public abstract Response clearRequested(boolean preserveSchema)
            throws Exception;

    public abstract void clear(boolean preserveSchema);

    public abstract Iterator<Map<String, Object>> iterator(String type)
            throws Exception;

    public abstract Iterator<Map<String, Object>> iterator(String type,
            SchemaDefinition schemaDefinition) throws Exception;

    public abstract Response exportAll() throws Exception;
}