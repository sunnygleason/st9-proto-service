package com.g414.st9.proto.service.cache;

public interface KeyValueCache {
	public abstract byte[] get(byte[] key) throws Exception;

	public abstract void put(byte[] key, byte[] value) throws Exception;

	public abstract void delete(byte[] key) throws Exception;

	public abstract void clear();
}