package com.g414.st9.proto.service.cache;

import java.util.Collection;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

/**
 * Memcached implementation of Ye Trusty Key Value Cache.
 */
public class MemcachedKeyValueCache implements KeyValueCache {
    private final MemcachedClient memcachedClient;
    private final int timeoutSecs = 15 * 60;

    public MemcachedKeyValueCache() throws Exception {
        this.memcachedClient = new MemcachedClient(AddrUtil.getAddresses(System
                .getProperty("memcached.host", "localhost:11211")));
    }

    public byte[] get(final String key) throws Exception {
        return (byte[]) memcachedClient.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, byte[]> multiget(final Collection<String> keys)
            throws Exception {
        Map<String, ?> result = memcachedClient.getBulk(keys);

        return (Map<String, byte[]>) result;
    }

    @Override
    public void put(final String key, final byte[] value) throws Exception {
        memcachedClient.set(key, timeoutSecs, value);
    }

    @Override
    public void delete(final String key) throws Exception {
        memcachedClient.delete(key);
    }

    @Override
    public void clear() {
        memcachedClient.flush();
    }

    @Override
    public boolean isPersistent() {
        return true;
    }
}