package com.g414.st9.proto.service.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

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
		byte[] key = "foo:1".getBytes();

		Map<String, Object> value = new LinkedHashMap<String, Object>();
		value.put("isAwesome", Boolean.TRUE);
		value.put("theAnswer", Integer.valueOf(42));

		byte[] valueBytes = EncodingHelper.convertToSmileLzf(value);

		assertNull(keyValueCache.get(key));

		keyValueCache.put(key, valueBytes);

		assertEquals(EncodingHelper.parseSmileLzf(keyValueCache.get(key)),
				value);

		keyValueCache.delete(key);

		assertNull(keyValueCache.get(key));

		keyValueCache.put(key, valueBytes);

		assertNotNull(keyValueCache.get(key));

		keyValueCache.clear();

		assertNull(keyValueCache.get(key));
	}
}
