package com.g414.st9.proto.service.store;

import javax.ws.rs.core.Response;

public interface KeyValueStorage {

	public abstract Response create(String type, String inValue)
			throws Exception;

	public abstract Response retrieve(String key) throws Exception;

	public abstract Response update(String key, String inValue)
			throws Exception;

	public abstract Response delete(String key) throws Exception;

	public abstract void clear();

}