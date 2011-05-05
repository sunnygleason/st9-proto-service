package utest.com.g414.st9.proto.service.index;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.IDBI;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.SecondaryIndexResource;
import com.g414.st9.proto.service.ServiceModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.index.OpaquePaginationHelper;
import com.g414.st9.proto.service.store.EncodingHelper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

@Test
public abstract class SecondaryIndexQueryTestBase {
    protected final String schema4 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"}],"
            + "\"indexes\":[{\"name\":\"xy\",\"cols\":["
            + "{\"name\":\"x\",\"sort\":\"ASC\"},{\"name\":\"y\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected final String schema4_isAwesome = "{\"version\":\"1\",\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"},{\"name\":\"isAwesome\",\"type\":\"BOOLEAN\"}],"
            + "\"indexes\":[{\"name\":\"xy\",\"cols\":["
            + "{\"name\":\"isAwesome\",\"sort\":\"ASC\"},{\"name\":\"x\",\"sort\":\"ASC\"},{\"name\":\"y\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected final String schema5 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"}],"
            + "\"indexes\":[{\"name\":\"xy\",\"cols\":["
            + "{\"name\":\"x\",\"sort\":\"DESC\"},{\"name\":\"y\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"DESC\"}]}]}";

    protected final String schema6 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"ref\",\"type\":\"REFERENCE\"}],"
            + "\"indexes\":[{\"name\":\"xref\",\"cols\":["
            + "{\"name\":\"x\",\"sort\":\"ASC\"},{\"name\":\"ref\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected final String schema7 = "{\"attributes\":[{\"name\":\"small\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"text\",\"type\":\"UTF8_TEXT\"}],"
            + "\"indexes\":[{\"name\":\"xref\",\"cols\":["
            + "{\"name\":\"small\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected KeyValueResource kvResource;
    protected SecondaryIndexResource indexResource;
    protected JDBISecondaryIndex index;
    protected SchemaResource schemaResource;
    protected IDBI database;

    public abstract Module getKeyValueStorageModule();

    public SecondaryIndexQueryTestBase() {
        Injector injector = Guice.createInjector(new LifecycleModule(),
                getKeyValueStorageModule(), new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(KeyValueCache.class).toInstance(
                                new EmptyKeyValueCache());
                    }
                }, new ServiceModule());

        this.database = injector.getInstance(IDBI.class);
        this.kvResource = injector.getInstance(KeyValueResource.class);
        this.indexResource = injector.getInstance(SecondaryIndexResource.class);
        this.index = injector.getInstance(JDBISecondaryIndex.class);
        this.schemaResource = injector.getInstance(SchemaResource.class);

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

    public void testQueryAsc() throws Exception {
        runSchemaTest("foo1", "xy", schema4);
    }

    public void testQueryDesc() throws Exception {
        runSchemaTest("foo2", "xy", schema5);
    }

    public void testIndexExists() throws Exception {
        Assert.assertTrue(!this.index.indexExists(database, "foo3", "yy"));
    }

    public void testMissingType() throws Exception {
        this.schemaResource.createEntity("foo4", schema4);

        Response r = this.indexResource.retrieveEntity("bar", "xy", "x eq 1",
                null, null);
        Assert.assertEquals(r.getEntity().toString(),
                "schema or index not found bar.xy");
    }

    public void testMissingIndex() throws Exception {
        this.schemaResource.createEntity("foo5", schema4);

        Response r = this.indexResource.retrieveEntity("foo5", "xyz", "x eq 1",
                null, null);
        Assert.assertEquals(r.getEntity().toString(),
                "schema or index not found foo5.xyz");
    }

    public void testReferenceType() throws Exception {
        String type = "foo6";
        this.schemaResource.createEntity(type, schema6);

        Response r1 = this.kvResource.createEntity(type,
                "{\"x\":1,\"ref\":\"foo:1\",\"isAwesome\":true}");
        Assert.assertEquals(
                r1.getEntity(),
                "{\"id\":\"@foo6:e5cbb6f9271a64f5\",\"kind\":\"foo6\",\"version\":\"1\",\"x\":1,\"ref\":\"foo:1\",\"isAwesome\":true}");

        Response r2 = this.kvResource.retrieveEntity("foo6:1");
        Assert.assertEquals(
                r2.getEntity(),
                "{\"id\":\"@foo6:e5cbb6f9271a64f5\",\"kind\":\"foo6\",\"version\":\"1\",\"x\":1,\"ref\":\"@foo:190272f987c6ac27\",\"isAwesome\":true}");
    }

    public void testUtf8Types() throws Exception {
        String type = "foo7";
        this.schemaResource.createEntity(type, schema7);

        Assert.assertTrue(this.index.indexExists(database, type, "xref"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"small\":\"" + i + "\",\"text\":\"" + -i + " even? "
                            + (i % 2 == 0) + "\"}").getEntity();
        }

        Response r = this.indexResource.retrieveEntity("foo7", "xref",
                "small eq \"1\"", null, null);
        Assert.assertEquals(
                r.getEntity().toString(),
                "{\"kind\":\"foo7\",\"index\":\"xref\",\"query\":\"small eq \\\"1\\\"\",\"results\":[{\"id\":\"@foo7:d53307ca898701db\"}],\"pageSize\":25,\"next\":null,\"prev\":null}");
    }

    public void testSchemaMigrate() throws Exception {
        runSchemaTest("foo1", "xy", schema4);

        Response r1 = this.indexResource.retrieveEntity("foo1", "xy",
                "isAwesome eq true and x gt -1", null, null);

        Assert.assertEquals(Status.BAD_REQUEST.getStatusCode(), r1.getStatus());
        Assert.assertEquals("'isAwesome' not in index", r1.getEntity());

        Response r2 = this.schemaResource.updateEntity("foo1",
                schema4_isAwesome);
        Assert.assertEquals(Status.OK.getStatusCode(), r2.getStatus());

        Map<String, Object> before = EncodingHelper
                .parseJsonString(schema4_isAwesome);
        before.remove("version");

        Map<String, Object> after = EncodingHelper.parseJsonString((String) r2
                .getEntity());
        after.remove("id");
        after.remove("kind");
        after.remove("version");

        Assert.assertEquals(before, after);

        Map<String, Object> result1 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity("foo1", "xy",
                                "isAwesome eq true and x gt -1", null, null)
                        .getEntity().toString());
        Assert.assertTrue(((List<?>) result1.get("results")).size() == OpaquePaginationHelper.DEFAULT_PAGE_SIZE);
        Assert.assertNotNull(result1.get("next"));
        Assert.assertNull(result1.get("prev"));

        Map<String, Object> result2 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity("foo1", "xy",
                                "isAwesome eq false and x gt -1", null, null)
                        .getEntity().toString());
        Assert.assertTrue(((List<?>) result2.get("results")).size() == OpaquePaginationHelper.DEFAULT_PAGE_SIZE);
        Assert.assertNotNull(result2.get("next"));
        Assert.assertNull(result2.get("prev"));
    }

    protected void runSchemaTest(String type, String indexName, String schema)
            throws Exception {
        this.schemaResource.createEntity(type, schema);

        Assert.assertTrue(this.index.indexExists(database, type, indexName));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"x\":" + i + ",\"y\":" + -i + ",\"isAwesome\":"
                            + (i % 2 == 0) + "}").getEntity();
        }

        Map<String, Object> result0 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt 1000", null, null)
                        .getEntity().toString());
        Assert.assertTrue(((List<?>) result0.get("results")).size() == 0);
        Assert.assertNull(result0.get("next"));
        Assert.assertNull(result0.get("prev"));

        Map<String, Object> result1 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt -1", null, null)
                        .getEntity().toString());
        Assert.assertTrue(((List<?>) result1.get("results")).size() == OpaquePaginationHelper.DEFAULT_PAGE_SIZE);
        Assert.assertNotNull(result1.get("next"));
        Assert.assertNull(result1.get("prev"));

        Map<String, Object> result2 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt -1",
                                (String) result1.get("next"), null).getEntity()
                        .toString());
        Assert.assertTrue(((List<?>) result2.get("results")).size() == OpaquePaginationHelper.DEFAULT_PAGE_SIZE);

        Assert.assertNotNull(result2.get("next"));
        Assert.assertNotNull(result2.get("prev"));

        Map<String, Object> result3 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt -1",
                                (String) result2.get("next"), null).getEntity()
                        .toString());

        Assert.assertTrue(((List<?>) result3.get("results")).size() == OpaquePaginationHelper.DEFAULT_PAGE_SIZE - 1L);

        Assert.assertNull(result3.get("next"));
        Assert.assertNotNull(result3.get("prev"));
    }
}