package com.g414.st9.proto.service.helper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisTemplates {
    public static <T> T withJedisCallback(JedisPool jedisPool,
            JedisCallback<T> jedisCallback) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedisCallback.withJedis(jedis);
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    public interface JedisCallback<T> {
        public T withJedis(Jedis jedis);
    }
}
