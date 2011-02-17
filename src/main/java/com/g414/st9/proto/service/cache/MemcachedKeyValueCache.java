package com.g414.st9.proto.service.cache;

import net.spy.memcached.MemcachedClient;

import org.apache.commons.codec.net.URLCodec;

import com.google.inject.Inject;

public class MemcachedKeyValueCache implements KeyValueCache {
	private final URLCodec codec = new URLCodec();
	private final MemcachedClient memcachedClient;
	private final String keyPrefix;
	private final int timeout = 15 * 60;

	@Inject
	public MemcachedKeyValueCache(MemcachedClient memcachedClient,
			String keyPrefix) {
		this.memcachedClient = memcachedClient;
		this.keyPrefix = keyPrefix;
	}

	@Override
	public byte[] get(final byte[] key) throws Exception {
		return (byte[]) memcachedClient.get(toMemcachedKey(key));
	}

	@Override
	public void put(final byte[] key, final byte[] value) throws Exception {
		memcachedClient.set(toMemcachedKey(key), timeout, value);
	}

	@Override
	public void delete(final byte[] key) throws Exception {
		memcachedClient.delete(toMemcachedKey(key));
	}

	@Override
	public void clear() {
		memcachedClient.flush();
	}

	private String toMemcachedKey(final byte[] key) {
		return keyPrefix + new String(codec.encode(key));
	}
}