package itest.com.g414.st9.proto.service.cache;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.cache.MemcachedKeyValueCache;

@Test
public class MemcachedKeyValueCacheTest extends KeyValueCacheTestBase {
    public MemcachedKeyValueCacheTest() throws Exception {
        super(new MemcachedKeyValueCache(new MemcachedClient(
                AddrUtil.getAddresses("localhost:11211")), "test:"));
    }
}
