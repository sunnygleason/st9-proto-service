package com.g414.st9.proto.service.cache;

import java.util.Collection;
import java.util.Map;

import net.spy.memcached.MemcachedClient;

import com.google.inject.Inject;

public class MemcachedKeyValueCache implements KeyValueCache {
    private final MemcachedClient memcachedClient;
    private final int timeout = 15 * 60;

    @Inject
    public MemcachedKeyValueCache(MemcachedClient memcachedClient,
            String keyPrefix) {
        this.memcachedClient = memcachedClient;
    }

    public byte[] get(final String key) throws Exception {
        return (byte[]) memcachedClient.get(key);
    }

    @Override
    public Map<String, byte[]> multiget(final Collection<String> keys)
            throws Exception {
        Map result = memcachedClient.getBulk(keys);

        return (Map<String, byte[]>) result;
    }

    @Override
    public void put(final String key, final byte[] value) throws Exception {
        memcachedClient.set(key, timeout, value);
    }

    @Override
    public void delete(final String key) throws Exception {
        memcachedClient.delete(key);
    }

    @Override
    public void clear() {
        memcachedClient.flush();
    }
}