package com.g414.st9.proto.service.cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis implementation of Ye Trusty Key Value Cache.
 */
public class RedisKeyValueCache implements KeyValueCache {
    private final JedisPool jedisPool;
    private final int timeoutSecs = 15 * 60;

    public RedisKeyValueCache() {
        this.jedisPool = new JedisPool(System.getProperty("redis.host",
                "localhost"), Integer.parseInt(System.getProperty("redis.port",
                "6379")));
    }

    @Override
    public byte[] get(final String key) throws Exception {
        return withJedisCallback(new JedisCallback<byte[]>() {
            @Override
            public byte[] withJedis(Jedis jedis) {
                return jedis.get(key.getBytes());
            }
        });
    }

    @Override
    public Map<String, byte[]> multiget(final Collection<String> keys)
            throws Exception {
        return withJedisCallback(new JedisCallback<Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> withJedis(Jedis jedis) {
                byte[][] keyBytes = new byte[keys.size()][];
                int i = 0;
                for (String key : keys) {
                    keyBytes[i++] = key.getBytes();
                }

                Map<String, byte[]> result = new LinkedHashMap<String, byte[]>();

                List<byte[]> resultBytes = jedis.mget(keyBytes);
                int j = 0;
                for (String key : keys) {
                    result.put(key, resultBytes.get(j++));
                }

                return result;
            }
        });
    }

    @Override
    public void put(final String key, final byte[] value) throws Exception {
        withJedisCallback(new JedisCallback<Void>() {
            @Override
            public Void withJedis(Jedis jedis) {
                jedis.setex(key.getBytes(), timeoutSecs, value);

                return null;
            }
        });
    }

    @Override
    public void delete(final String key) throws Exception {
        withJedisCallback(new JedisCallback<Void>() {
            @Override
            public Void withJedis(Jedis jedis) {
                jedis.del(key);

                return null;
            }
        });
    }

    @Override
    public void clear() {
        withJedisCallback(new JedisCallback<Void>() {
            @Override
            public Void withJedis(Jedis jedis) {
                jedis.flushDB();

                return null;
            }
        });
    }

    private <T> T withJedisCallback(JedisCallback<T> jedisCallback) {
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

    private interface JedisCallback<T> {
        public T withJedis(Jedis jedis);
    }
}