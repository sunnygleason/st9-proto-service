package utest.com.g414.st9.proto.service.index;

import java.util.List;
import java.util.Map;

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

    protected final String schema5 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"}],"
            + "\"indexes\":[{\"name\":\"xy\",\"cols\":["
            + "{\"name\":\"x\",\"sort\":\"DESC\"},{\"name\":\"y\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"DESC\"}]}]}";

    protected KeyValueResource kvResource;
    protected SecondaryIndexResource indexResource;
    protected SchemaResource schemaResource;

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

        this.kvResource = injector.getInstance(KeyValueResource.class);
        this.indexResource = injector.getInstance(SecondaryIndexResource.class);
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
        runSchemaTest("foo1", schema4);
    }

    public void testQueryDesc() throws Exception {
        runSchemaTest("foo2", schema5);
    }

    protected void runSchemaTest(String type, String schema) throws Exception {
        this.schemaResource.createEntity(type, schema);

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