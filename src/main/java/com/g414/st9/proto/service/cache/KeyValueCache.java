package com.g414.st9.proto.service.cache;

import java.util.Collection;
import java.util.Map;

public interface KeyValueCache {
    public abstract byte[] get(String key) throws Exception;

    public Map<String, byte[]> multiget(final Collection<String> keys)
            throws Exception;

    public abstract void put(String key, byte[] value) throws Exception;

    public abstract void delete(String key) throws Exception;

    public abstract void clear();
}