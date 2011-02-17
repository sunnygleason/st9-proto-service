package com.g414.st9.proto.service.cache;

import com.google.inject.Inject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisKeyValueCache implements KeyValueCache {
	private final JedisPool jedisPool;
	private final int timeout = 15 * 60;

	@Inject
	public RedisKeyValueCache(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	@Override
	public byte[] get(final byte[] key) throws Exception {
		return withJedisCallback(new JedisCallback<byte[]>() {
			@Override
			public byte[] withJedis(Jedis jedis) {
				return jedis.get(key);
			}
		});
	}

	@Override
	public void put(final byte[] key, final byte[] value) throws Exception {
		withJedisCallback(new JedisCallback<Void>() {
			@Override
			public Void withJedis(Jedis jedis) {
				jedis.setex(key, timeout, value);

				return null;
			}
		});
	}

	@Override
	public void delete(final byte[] key) throws Exception {
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