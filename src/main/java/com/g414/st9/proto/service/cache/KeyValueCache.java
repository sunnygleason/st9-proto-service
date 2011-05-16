package com.g414.st9.proto.service.cache;

import java.util.Collection;
import java.util.Map;

/**
 * Interface for key-value cache instances. The idea here is that we make
 * something that translates well to memcached, redis, etc. The major assumption
 * in this class is that the caller is doing the string munging to ensure there
 * are no key namespace collisions (for example, by adding a prefix to the key).
 */
public interface KeyValueCache {
    /** get the byte[] value associated with the specified key */
    public byte[] get(String key) throws Exception;

    /** multi-get the byte[] values associated with the specified keys */
    public Map<String, byte[]> multiget(final Collection<String> keys)
            throws Exception;

    /** set the byte[] value associated with the specified key */
    public void put(String key, byte[] value) throws Exception;

    /** remove the specified key entry from the cache */
    public void delete(String key) throws Exception;

    /** clear all entries from the cache (warning: for development only) */
    public void clear();

    /** returns true if the implementation actually attempts to store values */
    public boolean isPersistent();
}