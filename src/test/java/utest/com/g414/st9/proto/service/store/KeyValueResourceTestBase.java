package utest.com.g414.st9.proto.service.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.ServiceModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache.EmptyKeyValueCacheModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

@Test
public abstract class KeyValueResourceTestBase {
    private KeyValueResource kvResource;

    public abstract Module getKeyValueStorageModule();

    public KeyValueResourceTestBase() {
        Injector injector = Guice.createInjector(new LifecycleModule(),
                getKeyValueStorageModule(), new EmptyKeyValueCacheModule(),
                new ServiceModule());

        this.kvResource = injector.getInstance(KeyValueResource.class);

        injector.getInstance(Lifecycle.class).init();
        injector.getInstance(Lifecycle.class).start();
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        this.kvResource.clear();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        this.kvResource.clear();
    }

    public void testCreateHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:1\",\"kind\":\"bar\"}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:2\",\"kind\":\"bar\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:1"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:2"), Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("bar:1"), Status.OK,
                "{\"id\":\"bar:1\",\"kind\":\"bar\"}");

        assertResponseMatches(kvResource.retrieveEntity("bar:2"), Status.OK,
                "{\"id\":\"bar:2\",\"kind\":\"bar\"}");
    }

    public void testRetrieveHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("foo:1"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:2"), Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("foo:3"),
                Status.NOT_FOUND, "");
    }

    public void testUpdateHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(
                kvResource.updateEntity("foo:1", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(
                kvResource.updateEntity("foo:2", "{\"isAwesome\":false}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":false}");

        assertResponseMatches(
                kvResource.updateEntity("foo:3", "{\"isAwesome\":false}"),
                Status.NOT_FOUND, "");
    }

    public void testDeleteHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("foo:1"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:2"), Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

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

    public void testIteratorHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertTrue(kvResource.iterator("foo").hasNext());

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:1\",\"kind\":\"bar\"}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:2\",\"kind\":\"bar\"}");

        Iterator<Map<String, Object>> fooIter = kvResource.iterator("foo");
        assertEquals("foo", fooIter.next().get("kind"));
        assertEquals("foo", fooIter.next().get("kind"));
        assertFalse(fooIter.hasNext());

        Iterator<Map<String, Object>> barIter = kvResource.iterator("bar");
        assertEquals("bar", barIter.next().get("kind"));
        assertEquals("bar", barIter.next().get("kind"));
        assertFalse(barIter.hasNext());
    }

    public void testCreateFailureBadKeyWithId() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo:1", "{}"),
                Status.BAD_REQUEST, "Invalid entity 'type'");
    }

    public void testCreateFailureBadKeyNull() throws Exception {
        assertResponseMatches(kvResource.createEntity(null, "{}"),
                Status.BAD_REQUEST, "Invalid entity 'type'");
    }

    public void testCreateFailureBadKeyEmpty() throws Exception {
        assertResponseMatches(kvResource.createEntity("", "{}"),
                Status.BAD_REQUEST, "Invalid entity 'type'");
    }

    public void testCreateFailureBadValueNull() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", null),
                Status.BAD_REQUEST, "Invalid entity 'value'");
    }

    public void testCreateFailureBadValueEmpty() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", ""),
                Status.BAD_REQUEST, "Invalid entity 'value'");
    }

    @Test(expectedExceptions = WebApplicationException.class)
    public void testCreateFailureBadValueJson() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        kvResource.updateEntity("foo", "{bad value}");
    }

    public void testRetrieveFailureBadKey() throws Exception {
        assertResponseMatches(kvResource.retrieveEntity("foo"),
                Status.BAD_REQUEST, "Invalid entity 'id'");
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
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        kvResource.updateEntity("foo", null);
    }

    @Test(expectedExceptions = WebApplicationException.class)
    public void testUpdateFailureBadValueEmpty() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        kvResource.updateEntity("foo", "");
    }

    @Test(expectedExceptions = WebApplicationException.class)
    public void testUpdateFailureBadValueJson() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

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