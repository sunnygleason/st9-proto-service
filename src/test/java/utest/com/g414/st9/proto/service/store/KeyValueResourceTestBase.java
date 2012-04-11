package utest.com.g414.st9.proto.service.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.ServiceModule;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.pubsub.NoOpPublisher;
import com.g414.st9.proto.service.pubsub.Publisher;
import com.g414.st9.proto.service.schema.SchemaHelper;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

@Test
public abstract class KeyValueResourceTestBase {
    private SchemaResource schemaResource;
    private KeyValueResource kvResource;
    private KeyValueStorage store;
    private EmptyWriteThroughKeyValueCache cache;
    private ObjectMapper mapper = new ObjectMapper();

    public abstract Module getKeyValueStorageModule();

    public KeyValueResourceTestBase() {
        Injector injector = Guice.createInjector(new LifecycleModule(),
                getKeyValueStorageModule(), new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(KeyValueCache.class).toInstance(
                                new EmptyWriteThroughKeyValueCache());
                        bind(Publisher.class).toInstance(new NoOpPublisher());
                    }
                }, new ServiceModule());

        this.schemaResource = injector.getInstance(SchemaResource.class);
        this.kvResource = injector.getInstance(KeyValueResource.class);
        this.store = injector.getInstance(KeyValueStorage.class);
        this.cache = (EmptyWriteThroughKeyValueCache) injector
                .getInstance(KeyValueCache.class);

        injector.getInstance(Lifecycle.class).init();
        injector.getInstance(Lifecycle.class).start();
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        this.kvResource.clear();
        this.schemaResource.createEntity("foo",
                mapper.writeValueAsString(SchemaHelper.getEmptySchema()));
        this.schemaResource.createEntity("bar",
                mapper.writeValueAsString(SchemaHelper.getEmptySchema()));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        this.kvResource.clear();
    }

    public void testCreateHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:1\",\"kind\":\"bar\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:2\",\"kind\":\"bar\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:1", false),
                Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:2", false),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("bar:1", false),
                Status.OK,
                "{\"id\":\"bar:1\",\"kind\":\"bar\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.retrieveEntity("bar:2", false),
                Status.OK,
                "{\"id\":\"bar:2\",\"kind\":\"bar\",\"version\":\"1\"}");

        for (Map<String, Object> value : cache.getValues().values()) {
            Assert.assertEquals(value.get("version"), "1");
        }
    }

    public void testCreateSkipHappy() throws Exception {
        assertResponseMatches(store.create("foo", "{}", null, null, false),
                Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");
        assertResponseMatches(store.create("foo", "{}", null, null, false),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\"}");
        assertResponseMatches(store.create("foo", "{}", 6L, null, false),
                Status.OK,
                "{\"id\":\"foo:6\",\"kind\":\"foo\",\"version\":\"1\"}");
        assertResponseMatches(store.create("foo", "{}", null, null, false),
                Status.OK,
                "{\"id\":\"foo:7\",\"kind\":\"foo\",\"version\":\"1\"}");
    }

    public void testRetrieveHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("foo:1", false),
                Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:2", false),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("foo:3", false),
                Status.NOT_FOUND, "");
    }

    public void testMultiRetrieveHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\"}");
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:3\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertMultiResponseMatches(
                kvResource.retrieveEntity(
                        Lists.newArrayList("foo:1", "foo:2", "foo:3", "foo:5"),
                        false),
                Status.OK,
                "{\"foo:1\":{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"},"
                        + "\"foo:2\":{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\"},"
                        + "\"foo:3\":{\"id\":\"foo:3\",\"kind\":\"foo\",\"version\":\"1\"},"
                        + "\"foo:5\":null}");
    }

    public void testUpdateHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.updateEntity("foo:1",
                "{\"version\":\"1\",\"isAwesome\":true}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"2\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.updateEntity("foo:2",
                "{\"version\":\"1\",\"isAwesome\":false}"), Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"2\",\"isAwesome\":false}");

        assertResponseMatches(kvResource.updateEntity("foo:2",
                "{\"version\":\"1\",\"isAwesome\":false}"), Status.CONFLICT,
                "version conflict");

        assertResponseMatches(
                kvResource.updateEntity("foo:3", "{\"isAwesome\":false}"),
                Status.BAD_REQUEST, "missing 'version'");

        assertResponseMatches(kvResource.updateEntity("foo:3",
                "{\"isAwesome\":false,\"version\":\"1\"}"), Status.NOT_FOUND,
                "");

        for (Map<String, Object> value : cache.getValues().values()) {
            Assert.assertEquals(value.get("version"),
                    value.containsKey("isAwesome") ? "2" : "1");
        }
    }

    public void testDeleteHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.retrieveEntity("foo:1", false),
                Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.retrieveEntity("foo:2", false),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.deleteEntity("foo:1"),
                Status.NO_CONTENT, "");

        assertResponseMatches(kvResource.deleteEntity("foo:2"),
                Status.NO_CONTENT, "");

        assertResponseMatches(kvResource.deleteEntity("foo:3"),
                Status.NOT_FOUND, "");

        assertResponseMatches(kvResource.retrieveEntity("foo:1", false),
                Status.NOT_FOUND, "");

        assertResponseMatches(kvResource.retrieveEntity("foo:2", false),
                Status.NOT_FOUND, "");

    }

    public void testIteratorHappy() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertTrue(kvResource.iterator("foo").hasNext());

        assertResponseMatches(
                kvResource.createEntity("foo", "{\"isAwesome\":true}"),
                Status.OK,
                "{\"id\":\"foo:2\",\"kind\":\"foo\",\"version\":\"1\",\"isAwesome\":true}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:1\",\"kind\":\"bar\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.createEntity("bar", "{}"), Status.OK,
                "{\"id\":\"bar:2\",\"kind\":\"bar\",\"version\":\"1\"}");

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
                Status.BAD_REQUEST, "Invalid entity 'type': foo:1");
    }

    public void testCreateFailureBadKeyNull() throws Exception {
        assertResponseMatches(kvResource.createEntity(null, "{}"),
                Status.BAD_REQUEST, "Invalid entity 'type'");
    }

    public void testCreateFailureBadKeyEmpty() throws Exception {
        assertResponseMatches(kvResource.createEntity("", "{}"),
                Status.BAD_REQUEST, "Invalid entity 'type': ");
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
        assertResponseMatches(kvResource.retrieveEntity("foo", false),
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
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.updateEntity("foo:1", null),
                Status.BAD_REQUEST, "Invalid entity 'value'");
    }

    public void testUpdateFailureBadValueEmpty() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

        assertResponseMatches(kvResource.updateEntity("foo", ""),
                Status.BAD_REQUEST, "Invalid key");
    }

    public void testUpdateFailureBadValueJson() throws Exception {
        assertResponseMatches(kvResource.createEntity("foo", "{}"), Status.OK,
                "{\"id\":\"foo:1\",\"kind\":\"foo\",\"version\":\"1\"}");

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

    private static void assertMultiResponseMatches(Response r, Status status,
            String entity) throws Exception {
        Assert.assertEquals(r.getStatus(), status.getStatusCode());

        Map<String, Object> json1 = EncodingHelper.parseJsonString(r
                .getEntity().toString());
        Map<String, Object> json2 = EncodingHelper.parseJsonString(entity);

        Assert.assertEquals(json1.size(), json2.size());

        for (Map.Entry<String, Object> entry : json1.entrySet()) {
            Key id1 = Key.valueOf(entry.getKey());

            Map<String, Object> obj1 = (Map<String, Object>) entry.getValue();
            Map<String, Object> obj2 = (Map<String, Object>) json2.get(entry
                    .getKey());

            if (obj1 == null) {
                Assert.assertNull(obj2);
                continue;
            }

            String ids1 = (String) obj1.get("id");
            String ids2 = (String) obj2.get("id");

            if (ids1 != null) {
                Key rk1 = Key.valueOf(ids1);
                Key rk2 = Key.valueOf(ids2);

                Assert.assertEquals(rk1, rk2);
            }

            obj1.remove("id");
            obj2.remove("id");

            Assert.assertEquals(obj1, obj2);
        }
    }
}