package itest.com.g414.st9.proto.service.cache;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.cache.RedisKeyValueCache;

@Test
public class RedisKeyValueCacheTest extends KeyValueCacheTestBase {
    public RedisKeyValueCacheTest() throws Exception {
        super(new RedisKeyValueCache());
    }
}
