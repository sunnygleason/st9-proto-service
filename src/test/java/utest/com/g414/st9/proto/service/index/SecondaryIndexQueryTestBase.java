package utest.com.g414.st9.proto.service.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.IDBI;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.SecondaryIndexResource;
import com.g414.st9.proto.service.ServiceModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.store.Key;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

@Test
public abstract class SecondaryIndexQueryTestBase {
    protected final String schema4 = SchemaLoader.loadSchema("schema07");
    protected final String schema4_isAwesome = SchemaLoader
            .loadSchema("schema07b");
    protected final String schema5 = SchemaLoader.loadSchema("schema08");
    protected final String schema6 = SchemaLoader.loadSchema("schema09");
    protected final String schema7 = SchemaLoader.loadSchema("schema10");
    protected final String schema8 = SchemaLoader.loadSchema("schema11");
    protected final String schema9 = SchemaLoader.loadSchema("schema12");
    protected final String schema10 = SchemaLoader.loadSchema("schema13");
    protected final String schema11 = SchemaLoader.loadSchema("schema14");

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
        Assert.assertTrue(!this.index.tableExists(database, "foo3", "yy"));
    }

    public void testMissingType() throws Exception {
        safeCreateSchema("foo4", schema4);

        Response r1 = this.indexResource.retrieveEntity("bar", "xy", "x eq 1",
                null, null, false);
        Assert.assertEquals(r1.getEntity().toString(),
                "Invalid entity 'type': bar");

        Response r2 = this.indexResource.retrieveEntity("foo4", "xyz",
                "x eq 1", null, null, false);
        Assert.assertEquals(r2.getEntity().toString(),
                "schema or index not found foo4.xyz");
    }

    public void testMissingIndex() throws Exception {
        safeCreateSchema("foo5", schema4);

        Response r = this.indexResource.retrieveEntity("foo5", "xyz", "x eq 1",
                null, null, false);
        Assert.assertEquals(r.getEntity().toString(),
                "schema or index not found foo5.xyz");
    }

    public void testReferenceType() throws Exception {
        String type = "foo6";
        safeCreateSchema(type, schema6);

        Response r1 = this.kvResource.createEntity(type,
                "{\"x\":1,\"ref\":\"foo:1\",\"isAwesome\":true}");
        Assert.assertEquals(
                r1.getEntity(),
                "{\"id\":\"@foo6:e5cbb6f9271a64f5\",\"kind\":\"foo6\",\"version\":\"1\",\"x\":1,\"ref\":\"foo:1\",\"isAwesome\":true}");

        Response r2 = this.kvResource.retrieveEntity("foo6:1", false);
        Assert.assertEquals(
                r2.getEntity(),
                "{\"id\":\"@foo6:e5cbb6f9271a64f5\",\"kind\":\"foo6\",\"version\":\"1\",\"x\":1,\"ref\":\"@foo:190272f987c6ac27\",\"isAwesome\":true}");
    }

    public void testUtf8Types() throws Exception {
        String type = "foo7";
        safeCreateSchema(type, schema7);

        Assert.assertTrue(this.index.tableExists(database, type, "xref"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"small\":\"" + i + "\",\"text\":\"" + -i + " even? "
                            + (i % 2 == 0) + "\"}").getEntity();
        }

        Response r = this.indexResource.retrieveEntity(type, "xref",
                "small eq \"1\"", null, 25L, false);
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
                "isAwesome eq true and x gt -1", null, 25L, false);

        Assert.assertEquals(Status.BAD_REQUEST.getStatusCode(), r1.getStatus());
        Assert.assertEquals("'isAwesome' not in index", r1.getEntity());

        Response r2 = this.schemaResource.updateEntity("foo1",
                schema4_isAwesome, false);
        Assert.assertEquals(Status.OK.getStatusCode(), r2.getStatus());

        Map<String, Object> before = EncodingHelper
                .parseJsonString(schema4_isAwesome);
        before.remove("version");

        Response r3 = this.schemaResource.retrieveEntity("foo1");

        Map<String, Object> after = EncodingHelper.parseJsonString((String) r3
                .getEntity());
        after.remove("id");
        after.remove("kind");
        after.remove("version");

        Assert.assertEquals(after, before);

        Map<String, Object> result1 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity("foo1", "xy",
                                "isAwesome eq true and x gt -1", null, 25L,
                                false).getEntity().toString());
        Assert.assertTrue(((List<?>) result1.get("results")).size() == 25L);
        Assert.assertNotNull(result1.get("next"));
        Assert.assertNull(result1.get("prev"));

        Map<String, Object> result2 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity("foo1", "xy",
                                "isAwesome eq false and x gt -1", null, 25L,
                                false).getEntity().toString());
        Assert.assertTrue(((List<?>) result2.get("results")).size() == 25L);
        Assert.assertNotNull(result2.get("next"));
        Assert.assertNull(result2.get("prev"));
    }

    protected void runSchemaTest(String type, String indexName, String schema)
            throws Exception {
        safeCreateSchema(type, schema);
        Assert.assertTrue(this.index.tableExists(database, type, indexName));

        List<String> ids = new ArrayList<String>();

        for (int i = 0; i < 74; i++) {
            String ent = (String) this.kvResource.createEntity(
                    type,
                    "{\"x\":" + i + ",\"y\":" + -i + ",\"isAwesome\":"
                            + (i % 2 == 0) + "}").getEntity();

            ids.add((String) EncodingHelper.parseJsonString(ent).get("id"));
        }

        Map<String, Object> result0 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt 1000", null, 25L,
                                false).getEntity().toString());
        Assert.assertTrue(((List<?>) result0.get("results")).size() == 0);
        Assert.assertNull(result0.get("next"));
        Assert.assertNull(result0.get("prev"));

        Map<String, Object> result1 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt -1", null, 25L, false)
                        .getEntity().toString());
        Assert.assertTrue(((List<?>) result1.get("results")).size() == 25L);
        Assert.assertNotNull(result1.get("next"));
        Assert.assertNull(result1.get("prev"));

        Map<String, Object> result2 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt -1",
                                (String) result1.get("next"), 25L, false)
                        .getEntity().toString());
        Assert.assertTrue(((List<?>) result2.get("results")).size() == 25L);

        Assert.assertNotNull(result2.get("next"));
        Assert.assertNotNull(result2.get("prev"));

        Map<String, Object> result3 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy", "x gt -1",
                                (String) result2.get("next"), 25L, false)
                        .getEntity().toString());

        Assert.assertTrue(((List<?>) result3.get("results")).size() == 24L);

        Assert.assertNull(result3.get("next"));
        Assert.assertNotNull(result3.get("prev"));

        Map<String, Object> result4 = EncodingHelper
                .parseJsonString(this.indexResource
                        .retrieveEntity(type, "xy",
                                "x in (1, 2, 3, 4, -1) and y lt 0", null, 25L,
                                false).getEntity().toString());
        Assert.assertTrue(((List<?>) result4.get("results")).size() == 4);
        Assert.assertNull(result4.get("next"));
        Assert.assertNull(result4.get("prev"));

        Response result5 = this.indexResource.retrieveEntity(type, "xy",
                "x gt -1", "", 25L, false);
        Assert.assertEquals(result5.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(
                EncodingHelper.parseJsonString((String) result5.getEntity())
                        .get("pageSize"), 25);

        Response result6 = this.indexResource.retrieveEntity(type, "xy",
                "x gt -1", "-1", 25L, false);
        Assert.assertEquals(result6.getStatus(),
                Status.BAD_REQUEST.getStatusCode());
        Assert.assertEquals(result6.getEntity(), "invalid page token: -1");

        for (String id : ids) {
            Key k = Key.valueOf(id);

            Response r = this.kvResource.updateEntity(id,
                    "{\"version\":\"1\",\"x\":" + (-1 * k.getId()) + ",\"y\":"
                            + k.getId() + ",\"isAwesome\":"
                            + (k.getId() % 2 == 0) + "}");

            Assert.assertEquals(r.getStatus(), Status.OK.getStatusCode());
        }

        for (String id : ids) {
            Response r = this.kvResource.deleteEntity(id);

            Assert.assertEquals(r.getStatus(),
                    Status.NO_CONTENT.getStatusCode());
        }

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"x\":" + i + ",\"y\":" + -i + ",\"isAwesome\":"
                            + (i % 2 == 0) + "}").getEntity();
        }
    }

    protected void doUniqueIntegerTest(String type, String schema,
            Integer value, String found1, String found2) throws Exception {
        safeCreateSchema(type, schema);
        Assert.assertTrue(this.index.tableExists(database, type, "uniq"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(type, "{\"x\":" + value + "}")
                    .getEntity();
        }

        Response r = this.indexResource.retrieveEntity(type, "uniq", "x eq "
                + value, null, 25L, false);
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
        safeCreateSchema(type, schema);
        Assert.assertTrue(this.index.tableExists(database, type, "uniq"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"small\":\"" + prefix + i + "\",\"text\":\"" + -i
                            + " even? " + (i % 2 == 0) + "\"}").getEntity();
        }

        Response r = this.indexResource.retrieveEntity(type, "uniq",
                "small eq \"" + prefix + "1\"", null, 25L, false);
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
        safeCreateSchema(type, schema);
        Assert.assertTrue(this.index.tableExists(database, type,
                "uniq_compound"));

        for (int i = 0; i < 74; i++) {
            this.kvResource.createEntity(
                    type,
                    "{\"small\":\"" + prefix + i + "\",\"x\":" + i
                            + ",\"text\":\"" + -i + " even? " + (i % 2 == 0)
                            + "\"}").getEntity();
        }

        Response r0 = this.indexResource.retrieveEntity(type, "uniq_compound",
                "small eq \"" + prefix + "1\"", null, 25L, false);
        Assert.assertEquals(r0.getEntity().toString(),
                "unique index query must specify all fields");

        Response r1 = this.indexResource.retrieveEntity(type, "uniq_compound",
                "small eq \"" + prefix + "1\" and x eq 1", null, 25L, false);
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

    private void safeCreateSchema(String type, String schema) throws Exception {
        Response sr = this.schemaResource.createEntity(type, schema);
        if (sr.getStatus() != 200) {
            throw new IllegalStateException("schema didn't work:\n" + schema
                    + "\n" + sr.getEntity());
        }
    }
}