package utest.com.g414.st9.proto.service.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.store.EncodingHelper;

/**
 * This is the Tiniest Cache Ever(TM).
 */
public class EmptyWriteThroughKeyValueCache implements KeyValueCache {
    private final Map<String, Map<String, Object>> values = new HashMap<String, Map<String, Object>>();

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
        Map<String, Object> val = (Map<String, Object>) EncodingHelper
                .parseSmileLzf(value);

        values.put(key, val);
    }

    @Override
    public void delete(final String key) throws Exception {
    }

    @Override
    public void clear() {
        values.clear();
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    public Map<String, Map<String, Object>> getValues() {
        return Collections.unmodifiableMap(values);
    }
}