package com.g414.st9.proto.service.cache;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;

public class EmptyKeyValueCache implements KeyValueCache {
	@Override
	public byte[] get(final byte[] key) throws Exception {
		return null;
	}

	@Override
	public void put(final byte[] key, final byte[] value) throws Exception {
	}

	@Override
	public void delete(final byte[] key) throws Exception {
	}

	@Override
	public void clear() {
	}

	public static class EmptyKeyValueCacheModule extends AbstractModule {
		@Override
		public void configure() {
			Binder binder = binder();

			binder.bind(KeyValueCache.class).to(EmptyKeyValueCache.class);
		}
	}
}