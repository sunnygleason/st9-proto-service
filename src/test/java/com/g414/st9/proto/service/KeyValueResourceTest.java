package com.g414.st9.proto.service;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.ServiceConfig;
import com.google.inject.Injector;

@Test
public class KeyValueResourceTest {
	private KeyValueResource kvResource;

	public KeyValueResourceTest() {
		Injector injector = (new ServiceConfig()).getInjector();
		this.kvResource = injector.getInstance(KeyValueResource.class);
	}

	@BeforeMethod(alwaysRun = true)
	public void setUp() {
		this.kvResource.clear();
	}

	public void testCreateHappy() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(
				kvResource.createEntity("foo", "{\"isAwesome\":true}"),
				Status.OK, "{\"id\":\"foo:2\",\"isAwesome\":true}");

		assertResponseMatches(kvResource.retrieveEntity("foo:1"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(kvResource.retrieveEntity("foo:2"), Status.OK,
				"{\"id\":\"foo:2\",\"isAwesome\":true}");
	}

	public void testRetrieveHappy() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(
				kvResource.createEntity("foo", "{\"isAwesome\":true}"),
				Status.OK, "{\"id\":\"foo:2\",\"isAwesome\":true}");

		assertResponseMatches(kvResource.retrieveEntity("foo:1"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(kvResource.retrieveEntity("foo:2"), Status.OK,
				"{\"id\":\"foo:2\",\"isAwesome\":true}");

		assertResponseMatches(kvResource.retrieveEntity("foo:3"),
				Status.NOT_FOUND, "");
	}

	public void testUpdateHappy() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(
				kvResource.createEntity("foo", "{\"isAwesome\":true}"),
				Status.OK, "{\"id\":\"foo:2\",\"isAwesome\":true}");

		assertResponseMatches(
				kvResource.updateEntity("foo:1", "{\"isAwesome\":true}"),
				Status.OK, "{\"id\":\"foo:1\",\"isAwesome\":true}");

		assertResponseMatches(
				kvResource.updateEntity("foo:2", "{\"isAwesome\":false}"),
				Status.OK, "{\"id\":\"foo:2\",\"isAwesome\":false}");

		assertResponseMatches(
				kvResource.updateEntity("foo:3", "{\"isAwesome\":false}"),
				Status.NOT_FOUND, "");
	}

	public void testDeleteHappy() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(
				kvResource.createEntity("foo", "{\"isAwesome\":true}"),
				Status.OK, "{\"id\":\"foo:2\",\"isAwesome\":true}");

		assertResponseMatches(kvResource.retrieveEntity("foo:1"), Status.OK,
				"{\"id\":\"foo:1\"}");

		assertResponseMatches(kvResource.retrieveEntity("foo:2"), Status.OK,
				"{\"id\":\"foo:2\",\"isAwesome\":true}");

		assertResponseMatches(kvResource.deleteEntity("foo:1"),
				Status.NO_CONTENT, "");

		assertResponseMatches(kvResource.deleteEntity("foo:2"),
				Status.NO_CONTENT, "");

		assertResponseMatches(kvResource.deleteEntity("foo:3"),
				Status.NOT_FOUND, "");

		assertResponseMatches(kvResource.retrieveEntity("foo:1"),
				Status.NOT_FOUND, "");

		assertResponseMatches(kvResource.retrieveEntity("foo:2"),
				Status.NOT_FOUND, "");

	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testCreateFailureBadKeyWithId() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo:1", "{}"),
				Status.BAD_REQUEST, "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testCreateFailureBadKeyNull() throws Exception {
		assertResponseMatches(kvResource.createEntity(null, "{}"),
				Status.BAD_REQUEST, "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testCreateFailureBadKeyEmpty() throws Exception {
		assertResponseMatches(kvResource.createEntity("", "{}"),
				Status.BAD_REQUEST, "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testCreateFailureBadValueNull() throws Exception {
		kvResource.createEntity("foo", null);
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testCreateFailureBadValueEmpty() throws Exception {
		kvResource.createEntity("foo", "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testCreateFailureBadValueJson() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		kvResource.updateEntity("foo", "{bad value}");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testRetrieveFailureBadKey() throws Exception {
		assertResponseMatches(kvResource.retrieveEntity("foo"),
				Status.BAD_REQUEST, "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testUpdateFailureBadKeyNoId() throws Exception {
		kvResource.updateEntity("foo", "{}");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testUpdateFailureBadKeyNull() throws Exception {
		assertResponseMatches(kvResource.updateEntity(null, "{}"),
				Status.BAD_REQUEST, "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testUpdateFailureBadKeyEmpty() throws Exception {
		assertResponseMatches(kvResource.updateEntity("", "{}"),
				Status.BAD_REQUEST, "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testUpdateFailureBadValueNull() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		kvResource.updateEntity("foo", null);
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testUpdateFailureBadValueEmpty() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		kvResource.updateEntity("foo", "");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testUpdateFailureBadValueJson() throws Exception {
		assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
				"{\"id\":\"foo:1\"}");

		kvResource.updateEntity("foo", "{bad value}");
	}

	@Test(expectedExceptions = WebApplicationException.class)
	public void testDeleteFailureBadKey() throws Exception {
		assertResponseMatches(kvResource.deleteEntity("foo"),
				Status.BAD_REQUEST, "");
	}

	private static void assertResponseMatches(Response r, Status status,
			String entity) {
		Assert.assertEquals(r.getStatus(), status.getStatusCode());
		Assert.assertEquals(r.getEntity(), entity);
	}
}