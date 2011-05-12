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

    protected final String schema8 = "{\"attributes\":[{\"name\":\"small\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"text\",\"type\":\"UTF8_TEXT\"}],"
            + "\"indexes\":[{\"name\":\"uniq\",\"unique\":true,\"cols\":["
            + "{\"name\":\"small\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected final String schema9 = "{\"attributes\":[{\"name\":\"small\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"text\",\"type\":\"UTF8_TEXT\"}],"
            + "\"indexes\":[{\"name\":\"uniq\",\"unique\":true,\"cols\":["
            + "{\"name\":\"small\",\"transform\":\"LOWERCASE\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected final String schema10 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"}],"
            + "\"indexes\":[{\"name\":\"uniq\",\"unique\":true,\"cols\":["
            + "{\"name\":\"x\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

    protected final String schema11 = "{\"attributes\":[{\"name\":\"small\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"x\",\"type\":\"I32\"}],"
            + "\"indexes\":[{\"name\":\"uniq_compound\",\"unique\":true,\"cols\":["
            + "{\"name\":\"small\",\"sort\":\"ASC\"},{\"name\":\"x\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}";

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

        Response r = this.indexResource.retrieveEntity(type, "xref",
                "small eq \"1\"", null, null);
        Assert.assertEquals(
                r.getEntity().toString(),
                "{\"kind\":\"foo7\",\"index\":\"xref\",\"query\":\"small eq \\\"1\\\"\",\"results\":[{\"id\":\"@foo7:d53307ca898701db\"}],\"pageSize\":25,\"next\":null,\"prev\":null}");
    }

    public void testUniqueIntegerIndexes() throws Exception {
        doUniqueIntegerTest("foo10", schema10, 0, "@foo10:e882c1cbac34a48b",
                "@foo10:28aab6c4f2aeed54");
    }

    public void testUniqueStringIndexes() throws Exception {
        doUniqueStringTest("foo8", schema8, "small", "small",
                "@foo8:6c3b63e510ae10b3", "@foo8:02668c07cad2b4b7");
        doUniqueStringTest("foo9", schema9, "sMall", "small",
                "@foo9:0c9779abf62acd35", "@foo9:35690aaee888bd21");
    }

    public void testUniqueCompoundIndexes() throws Exception {
        doUniqueCompoundTest("foo11", schema11, "sMall", "small",
                "@foo11:55078e4057b0592b", "@foo11:d807404598cb174d",
                "@foo11:4ec8cf2fb620dc54");
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

    protected void doUniqueIntegerTest(String type, String schema,
            Integer value, String found1, String found2) throws Exception {
        this.schemaResource.createEntity(type, schema);

        Assert.assertTrue(this.index.indexExists(database, type, "uniq"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(type, "{\"x\":" + value + "}")
                    .getEntity();
        }

        Response r = this.indexResource.retrieveEntity(type, "uniq", "x eq "
                + value, null, null);
        Assert.assertEquals(r.getEntity().toString(), "{\"kind\":\"" + type
                + "\",\"index\":\"uniq\",\"query\":\"x eq " + value
                + "\",\"results\":[{\"id\":\"" + found1
                + "\"}],\"pageSize\":25,\"next\":null,\"prev\":null}");

        Response r2 = this.kvResource.createEntity(type, "{\"x\":" + value
                + "}");

        Assert.assertEquals(r2.getStatus(), Status.CONFLICT.getStatusCode());
        Assert.assertEquals(r2.getEntity().toString(),
                "unique index constraint violation");

        Response r3 = this.kvResource.createEntity(type, "{\"x\":"
                + (value + 1) + "}");

        Assert.assertEquals(r3.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(r3.getEntity().toString(), "{\"id\":\"" + found2
                + "\",\"kind\":\"" + type + "\",\"version\":\"1\",\"x\":"
                + (value + 1) + "}");
    }

    protected void doUniqueStringTest(String type, String schema,
            String prefix, String target, String found1, String found2)
            throws Exception {
        this.schemaResource.createEntity(type, schema);

        Assert.assertTrue(this.index.indexExists(database, type, "uniq"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"small\":\"" + prefix + i + "\",\"text\":\"" + -i
                            + " even? " + (i % 2 == 0) + "\"}").getEntity();
        }

        Response r = this.indexResource.retrieveEntity(type, "uniq",
                "small eq \"" + prefix + "1\"", null, null);
        Assert.assertEquals(r.getEntity().toString(), "{\"kind\":\"" + type
                + "\",\"index\":\"uniq\",\"query\":\"small eq \\\"" + prefix
                + "1\\\"\",\"results\":[{\"id\":\"" + found1
                + "\"}],\"pageSize\":25,\"next\":null,\"prev\":null}");

        Response r2 = this.kvResource.createEntity(type, "{\"small\":\""
                + target + 1 + "\",\"text\":\"" + -1 + " even? false\"}");

        Assert.assertEquals(r2.getStatus(), Status.CONFLICT.getStatusCode());
        Assert.assertEquals(r2.getEntity().toString(),
                "unique index constraint violation");

        Response r3 = this.kvResource.createEntity(type, "{\"small\":\""
                + prefix + 75 + "\",\"text\":\"" + -1 + " even? false\"}");

        Assert.assertEquals(r3.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(r3.getEntity().toString(), "{\"id\":\"" + found2
                + "\",\"kind\":\"" + type + "\",\"version\":\"1\",\"small\":\""
                + prefix + "75\",\"text\":\"-1 even? false\"}");
    }

    protected void doUniqueCompoundTest(String type, String schema,
            String prefix, String target, String found1, String found2,
            String found3) throws Exception {
        this.schemaResource.createEntity(type, schema);

        Assert.assertTrue(this.index.indexExists(database, type,
                "uniq_compound"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"small\":\"" + prefix + i + "\",\"x\":" + i
                            + ",\"text\":\"" + -i + " even? " + (i % 2 == 0)
                            + "\"}").getEntity();
        }

        Response r0 = this.indexResource.retrieveEntity(type, "uniq_compound",
                "small eq \"" + prefix + "1\"", null, null);
        Assert.assertEquals(r0.getEntity().toString(),
                "unique index query must specify all fields");

        Response r1 = this.indexResource.retrieveEntity(type, "uniq_compound",
                "small eq \"" + prefix + "1\" and x eq 1", null, null);
        Assert.assertEquals(r1.getEntity().toString(), "{\"kind\":\"" + type
                + "\",\"index\":\"uniq_compound\",\"query\":\"small eq \\\""
                + prefix + "1\\\" and x eq 1\",\"results\":[{\"id\":\""
                + found1 + "\"}],\"pageSize\":25,\"next\":null,\"prev\":null}");

        Response r2 = this.kvResource.createEntity(type, "{\"small\":\""
                + prefix + 1 + "\",\"x\":" + 1 + ",\"text\":\"" + -1
                + " even? false\"}");
        Assert.assertEquals(r2.getStatus(), Status.CONFLICT.getStatusCode());
        Assert.assertEquals(r2.getEntity().toString(),
                "unique index constraint violation");

        Response r3 = this.kvResource.createEntity(type, "{\"small\":\""
                + prefix + 2 + "\",\"x\":" + 1 + ",\"text\":\"" + -1
                + " even? false\"}");
        Assert.assertEquals(r3.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(r3.getEntity().toString(), "{\"id\":\"" + found2
                + "\",\"kind\":\"" + type + "\",\"version\":\"1\",\"small\":\""
                + prefix + "2\",\"x\":" + 1 + ",\"text\":\"-1 even? false\"}");

        Response r4 = this.kvResource.createEntity(type, "{\"small\":\""
                + prefix + 1 + "\",\"x\":" + 2 + ",\"text\":\"" + -1
                + " even? false\"}");
        Assert.assertEquals(r4.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(r4.getEntity().toString(), "{\"id\":\"" + found3
                + "\",\"kind\":\"" + type + "\",\"version\":\"1\",\"small\":\""
                + prefix + "1\",\"x\":" + 2 + ",\"text\":\"-1 even? false\"}");
    }
}