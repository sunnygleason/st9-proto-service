package com.g414.st9.proto.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;

/**
 * In-memory implementation of key-value storage. Since this is the only
 * implementation right now, no sense extracting an interface yet. Stay tuned...
 */
public class InMemoryKeyValueStorage {
	private final ConcurrentHashMap<String, byte[]> storage = new ConcurrentHashMap<String, byte[]>();
	private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<String, AtomicLong>();
	private final SmileFactory smileFactory = new SmileFactory();
	private final JsonFactory jsonFactory = new JsonFactory();
	private final ObjectMapper mapper = new ObjectMapper();

	public Response create(String type, String inValue) throws Exception {
		validateType(type);
		Map<String, Object> readValue = parseValue(inValue);

		String key = type + ":" + this.nextId(type);
		Map<String, Object> value = new LinkedHashMap<String, Object>();
		value.put("id", key);
		value.putAll(readValue);
		value.put("id", key);

		String valueJson = mapper.writeValueAsString(value);
		byte[] valueBytes = convertToSmile(value);

		storage.put(key, valueBytes);

		return Response.status(Status.OK).entity(valueJson).build();
	}

	public Response retrieve(String key) throws Exception {
		validateKey(key);

		byte[] valueBytes = storage.get(key);

		if (valueBytes == null) {
			return Response.status(Status.NOT_FOUND).entity("").build();
		}

		ByteArrayInputStream in = new ByteArrayInputStream(valueBytes);
		SmileParser smile = smileFactory.createJsonParser(in);

		LinkedHashMap<String, Object> value = (LinkedHashMap<String, Object>) mapper
				.readValue(smile, LinkedHashMap.class);

		StringWriter out = new StringWriter();
		JsonGenerator json = jsonFactory.createJsonGenerator(out);
		mapper.writeValue(json, value);

		return Response.status(Status.OK).entity(out.toString()).build();
	}

	public Response update(String key, String inValue) throws Exception {
		validateKey(key);
		Map<String, Object> readValue = parseValue(inValue);
		Map<String, Object> value = new LinkedHashMap<String, Object>();
		value.put("id", key);
		value.putAll(readValue);
		value.put("id", key);

		if (!storage.containsKey(key)) {
			return Response.status(Status.NOT_FOUND).entity("").build();
		}

		String valueJson = mapper.writeValueAsString(value);
		storage.put(key, convertToSmile(value));

		return Response.status(Status.OK).entity(valueJson).build();
	}

	public Response delete(String key) throws Exception {
		validateKey(key);

		if (!storage.containsKey(key)) {
			return Response.status(Status.NOT_FOUND).entity("").build();
		}

		storage.remove(key);

		return Response.status(Status.NO_CONTENT).entity("").build();
	}

	public void clear() {
		this.sequences.clear();
		this.storage.clear();
	}

	private Long nextId(String type) {
		validateType(type);

		AtomicLong aNewSeq = new AtomicLong(0);
		AtomicLong existing = sequences.putIfAbsent(type, aNewSeq);

		if (existing == null) {
			existing = aNewSeq;
		}

		return existing.incrementAndGet();
	}

	private Map<String, Object> parseValue(String value) {
		if (value == null || value.length() == 0) {
			throw new WebApplicationException(Response
					.status(Status.BAD_REQUEST)
					.entity("Invalid entity 'value'").build());
		}

		try {
			return (Map<String, Object>) mapper.readValue(value,
					LinkedHashMap.class);
		} catch (Exception e) {
			throw new WebApplicationException(Response
					.status(Status.BAD_REQUEST)
					.entity("Invalid entity 'value'").build());
		}
	}

	private byte[] convertToSmile(Object value) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			SmileGenerator smile = smileFactory.createJsonGenerator(out);
			mapper.writeValue(smile, value);

			return out.toByteArray();
		} catch (Exception e) {
			throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
		}
	}

	private static void validateType(String type) {
		if (type == null || type.length() == 0 || type.indexOf(":") != -1) {
			throw new WebApplicationException(Response
					.status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
					.build());
		}
	}

	private static void validateKey(String key) {
		if (key == null || key.length() == 0 || key.indexOf(":") == -1) {
			throw new WebApplicationException(Response
					.status(Status.BAD_REQUEST).entity("Invalid entity 'id'")
					.build());
		}
	}
}
