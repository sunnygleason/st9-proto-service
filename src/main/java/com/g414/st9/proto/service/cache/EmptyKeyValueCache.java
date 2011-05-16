package com.g414.st9.proto.service.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * This is the Tiniest Cache Ever(TM).
 */
public class EmptyKeyValueCache implements KeyValueCache {
    @Override
    public byte[] get(final String key) throws Exception {
        return null;
    }

    @Override
    public Map<String, byte[]> multiget(Collection<String> keys)
            throws Exception {
        return Collections.<String, byte[]> emptyMap();
    }

    @Override
    public void put(final String key, final byte[] value) throws Exception {
    }

    @Override
    public void delete(final String key) throws Exception {
    }

    @Override
    public void clear() {
    }

    @Override
    public boolean isPersistent() {
        return false;
    }
}