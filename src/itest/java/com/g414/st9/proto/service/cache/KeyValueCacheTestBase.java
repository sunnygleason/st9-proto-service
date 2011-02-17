package com.g414.st9.proto.service.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.store.EncodingHelper;

@Test
public abstract class KeyValueCacheTestBase {
    protected final KeyValueCache keyValueCache;

    public KeyValueCacheTestBase(KeyValueCache keyValueCache) {
        this.keyValueCache = keyValueCache;
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        keyValueCache.clear();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        keyValueCache.clear();
    }

    public void testGetSet() throws Exception {
        String key1 = "kv:foo:1";
        String key2 = "kv:bar:2";

        Map<String, Object> value1 = new LinkedHashMap<String, Object>();
        value1.put("isAwesome", Boolean.TRUE);
        value1.put("theAnswer", Integer.valueOf(42));

        byte[] value1Bytes = EncodingHelper.convertToSmileLzf(value1);

        assertNull(keyValueCache.get(key1));

        keyValueCache.put(key1, value1Bytes);

        assertEquals(EncodingHelper.parseSmileLzf(keyValueCache.get(key1)),
                value1);

        keyValueCache.delete(key1);

        assertNull(keyValueCache.get(key1));

        keyValueCache.put(key1, value1Bytes);

        assertNotNull(keyValueCache.get(key1));

        keyValueCache.clear();

        assertNull(keyValueCache.get(key1));

        Map<String, Object> value2 = new LinkedHashMap<String, Object>();
        value2.put("isAwesome", Boolean.FALSE);
        value2.put("theAnswer", Integer.valueOf(42));

        byte[] value2Bytes = EncodingHelper.convertToSmileLzf(value2);

        assertNull(keyValueCache.get(key2));

        keyValueCache.put(key1, value1Bytes);
        keyValueCache.put(key2, value2Bytes);

        Map<String, byte[]> multiResult = keyValueCache.multiget(Arrays.asList(
                key1, key2));
        assertEquals(EncodingHelper.parseSmileLzf(multiResult.get(key1)),
                value1);
        assertEquals(EncodingHelper.parseSmileLzf(multiResult.get(key2)),
                value2);
    }
}
