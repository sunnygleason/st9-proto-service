package itest.com.g414.st9.proto.service.cache;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.cache.MemcachedKeyValueCache;

@Test
public class MemcachedKeyValueCacheTest extends KeyValueCacheTestBase {
    public MemcachedKeyValueCacheTest() throws Exception {
        super(new MemcachedKeyValueCache());
    }
}
