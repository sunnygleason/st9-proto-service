package utest.com.g414.st9.proto.service.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

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
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.store.EncodingHelper;
import com.g414.st9.proto.service.store.Key;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

@Test
public abstract class KeyValueResourceTestBase {
    private KeyValueResource kvResource;

    public abstract Module getKeyValueStorageModule();

    public KeyValueResourceTestBase() {
        Injector injector = Guice.createInjector(new LifecycleModule(),
                getKeyValueStorageModule(), new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(KeyValueCache.class).toInstance(
                                new EmptyKeyValueCache());
                    }
                }, new ServiceModule());

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
        assertEquals("foo:1", Key.valueOf((String) fooIter.next().get("id"))
                .getIdentifier());
        assertEquals("foo:2", Key.valueOf((String) fooIter.next().get("id"))
                .getIdentifier());
        assertFalse(fooIter.hasNext());

        Iterator<Map<String, Object>> barIter = kvResource.iterator("bar");
        assertEquals("bar:1", Key.valueOf((String) barIter.next().get("id"))
                .getIdentifier());
        assertEquals("bar:2", Key.valueOf((String) barIter.next().get("id"))
                .getIdentifier());
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

    public void testCreateFailureBadValueJson() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{bad value}"),
                Status.BAD_REQUEST, "Invalid entity 'value'");
    }

    public void testRetrieveFailureBadKey() throws Exception {
        assertResponseMatches(kvResource.retrieveEntity("foo"),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testUpdateFailureBadKeyNoId() throws Exception {
        assertResponseMatches(kvResource.updateEntity("foo", "{}"),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testUpdateFailureBadKeyNull() throws Exception {
        assertResponseMatches(kvResource.updateEntity(null, "{}"),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testUpdateFailureBadKeyEmpty() throws Exception {
        assertResponseMatches(kvResource.updateEntity("", "{}"),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testUpdateFailureBadValueNull() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(kvResource.updateEntity("foo:1", null),
                Status.BAD_REQUEST, "Invalid entity 'value'");
    }

    public void testUpdateFailureBadValueEmpty() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(kvResource.updateEntity("foo", ""),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testUpdateFailureBadValueJson() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\"}");

        assertResponseMatches(kvResource.updateEntity("foo:1", "{bad value}"),
                Status.BAD_REQUEST, "Invalid entity 'value'");
    }

    public void testDeleteFailureBadKey() throws Exception {
        assertResponseMatches(kvResource.deleteEntity("foo"),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testDeleteFailureNullKey() throws Exception {
        assertResponseMatches(kvResource.deleteEntity(null),
                Status.BAD_REQUEST, "Invalid key");
    }

    private static void assertResponseMatches(Response r, Status status,
            String entity) throws Exception {
        Assert.assertEquals(r.getStatus(), status.getStatusCode());

        try {
            Map<String, Object> json1 = EncodingHelper.parseJsonString(r
                    .getEntity().toString());
            Map<String, Object> json2 = EncodingHelper.parseJsonString(entity);

            Key id1 = Key.valueOf((String) json1.get("id"));
            Key id2 = Key.valueOf((String) json2.get("id"));

            Assert.assertEquals(id1, id2);

            json1.remove("id");
            json2.remove("id");

            Assert.assertEquals(json1, json2);
        } catch (Exception e) {
            // in the event parsing JSON fails, fallback on String equality
            Assert.assertEquals(r.getEntity(), entity);
        }
    }
}