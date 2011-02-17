package com.g414.st9.proto.service.cache;

import org.testng.annotations.Test;

import redis.clients.jedis.JedisPool;

@Test
public class RedisKeyValueCacheTest extends KeyValueCacheTestBase {
    public RedisKeyValueCacheTest() throws Exception {
        super(new RedisKeyValueCache(new JedisPool("localhost", 6379)));
    }
}
