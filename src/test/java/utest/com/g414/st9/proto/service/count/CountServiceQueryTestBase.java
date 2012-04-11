package utest.com.g414.st9.proto.service.count;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.skife.jdbi.v2.IDBI;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.CounterResource;
import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.ServiceModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.count.JDBICountService;
import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.pubsub.NoOpPublisher;
import com.g414.st9.proto.service.pubsub.Publisher;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.store.Key;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.internal.ImmutableList;

@Test
public abstract class CountServiceQueryTestBase {
    protected final String schema4 = SchemaLoader.loadSchema("schema19");
    protected final String schema5 = SchemaLoader.loadSchema("schema20");

    protected final ObjectMapper mapper = new ObjectMapper();
    protected KeyValueResource kvResource;
    protected CounterResource countResource;
    protected JDBICountService counts;
    protected SchemaResource schemaResource;
    protected IDBI database;

    public abstract Module getKeyValueStorageModule();

    public CountServiceQueryTestBase() {
        Injector injector = Guice.createInjector(new LifecycleModule(),
                getKeyValueStorageModule(), new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(KeyValueCache.class).toInstance(
                                new EmptyKeyValueCache());
                        bind(Publisher.class).toInstance(new NoOpPublisher());
                    }
                }, new ServiceModule());

        this.database = injector.getInstance(IDBI.class);
        this.kvResource = injector.getInstance(KeyValueResource.class);
        this.countResource = injector.getInstance(CounterResource.class);
        this.counts = injector.getInstance(JDBICountService.class);
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

    public void testCounters00() throws Exception {
        runCounterTest("awesome", "ia", schema4, new SeedData() {
            @Override
            public Map<String, Object> getData(long seed) {
                return ImmutableMap.<String, Object> of("isAwesome", false);
            }
        }, new CounterAssertions() {
            @Override
            public void assertCounters(SchemaDefinition def) throws Exception {
                List<Map<String, Object>> result = counts.doCounterQuery(
                        database, "awesome", "ia",
                        Collections.<QueryTerm> emptyList(), null, 25L, def);

                jsoneq(result, ImmutableList.of(ImmutableMap.of("isAwesome",
                        false, "count", 74)));
            }
        });
    }

    public void testCounters01() throws Exception {
        runCounterTest("awesome", "ia", schema4, new SeedData() {
            @Override
            public Map<String, Object> getData(long seed) {
                return ImmutableMap.<String, Object> of("isAwesome",
                        (seed > 10));
            }
        }, new CounterAssertions() {
            @Override
            public void assertCounters(SchemaDefinition def) throws Exception {
                List<Map<String, Object>> result = counts.doCounterQuery(
                        database, "awesome", "ia",
                        Collections.<QueryTerm> emptyList(), null, 25L, def);

                jsoneq(result, ImmutableList.of(
                        ImmutableMap.of("isAwesome", false, "count", 11),
                        ImmutableMap.of("isAwesome", true, "count", 63)));
            }
        });
    }

    public void testCounters02() throws Exception {
        runCounterTest("awesome", "ia", schema4, new SeedData() {
            @Override
            public Map<String, Object> getData(long seed) {
                return ImmutableMap.<String, Object> of("isAwesome",
                        (seed < 10));
            }
        }, new CounterAssertions() {
            @Override
            public void assertCounters(SchemaDefinition def) throws Exception {
                List<Map<String, Object>> result = counts.doCounterQuery(
                        database, "awesome", "ia",
                        Collections.<QueryTerm> emptyList(), null, 25L, def);

                Assert.assertEquals(mapper.writeValueAsString(result), mapper
                        .writeValueAsString(ImmutableList.of(ImmutableMap.of(
                                "isAwesome", false, "count", 64), ImmutableMap
                                .of("isAwesome", true, "count", 10))));
            }
        });
    }

    public void testCounters03() throws Exception {
        runCounterTest("complex", "byAwesome", schema5, new SeedData() {
            @Override
            public Map<String, Object> getData(long seed) {
                return ImmutableMap.<String, Object> of("target", "foo:"
                        + (seed % 7), "isAwesome", (seed < 10), "hotness",
                        ((seed % 2 == 0) ? ((seed % 4 == 0) ? "TEH_HOTNESS"
                                : "WARM") : "COOL"), "year",
                        (1980 + (seed % 2)));
            }
        }, new CounterAssertions() {
            @Override
            public void assertCounters(SchemaDefinition def) throws Exception {
                List<Map<String, Object>> result01 = counts.doCounterQuery(
                        database, "complex", "byAwesome",
                        Collections.<QueryTerm> emptyList(), null, 25L, def);

                jsoneq(result01, ImmutableList.of(
                        ImmutableMap.of("isAwesome", false, "count", 64),
                        ImmutableMap.of("isAwesome", true, "count", 10)));

                List<Map<String, Object>> result02 = counts.doCounterQuery(
                        database, "complex", "byTarget",
                        Collections.<QueryTerm> emptyList(), null, 25L, def);

                jsoneq(result02, ImmutableList.of(ImmutableMap.of("target",
                        "@foo:190272f987c6ac27", "count", 11), ImmutableMap.of(
                        "target", "@foo:573c812fe6841168", "count", 11),
                        ImmutableMap.of("target", "@foo:5b7f06a2fbf79d07",
                                "count", 10), ImmutableMap.of("target",
                                "@foo:9c7897f5fe867388", "count", 10),
                        ImmutableMap.of("target", "@foo:ce4ad6a1cd6293d9",
                                "count", 11), ImmutableMap.of("target",
                                "@foo:deab185c6138b16e", "count", 11),
                        ImmutableMap.of("target", "@foo:f79fe6c8ee441b18",
                                "count", 10)));

                List<Map<String, Object>> result03 = counts.doCounterQuery(
                        database, "complex", "byTargetHotnessYear",
                        Collections.<QueryTerm> emptyList(), null, 25L, def);

                jsoneq(result03, ImmutableList.of(ImmutableMap.of("target",
                        "@foo:190272f987c6ac27", "hotness", "COOL", "year",
                        1981, "count", 6), ImmutableMap.of("target",
                        "@foo:190272f987c6ac27", "hotness", "WARM", "year",
                        1980, "count", 2), ImmutableMap.of("target",
                        "@foo:190272f987c6ac27", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 3), ImmutableMap.of("target",
                        "@foo:573c812fe6841168", "hotness", "COOL", "year",
                        1981, "count", 6), ImmutableMap.of("target",
                        "@foo:573c812fe6841168", "hotness", "WARM", "year",
                        1980, "count", 3), ImmutableMap.of("target",
                        "@foo:573c812fe6841168", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 2), ImmutableMap.of("target",
                        "@foo:5b7f06a2fbf79d07", "hotness", "COOL", "year",
                        1981, "count", 5), ImmutableMap.of("target",
                        "@foo:5b7f06a2fbf79d07", "hotness", "WARM", "year",
                        1980, "count", 2), ImmutableMap.of("target",
                        "@foo:5b7f06a2fbf79d07", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 3), ImmutableMap.of("target",
                        "@foo:9c7897f5fe867388", "hotness", "COOL", "year",
                        1981, "count", 5), ImmutableMap.of("target",
                        "@foo:9c7897f5fe867388", "hotness", "WARM", "year",
                        1980, "count", 3), ImmutableMap.of("target",
                        "@foo:9c7897f5fe867388", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 2), ImmutableMap.of("target",
                        "@foo:ce4ad6a1cd6293d9", "hotness", "COOL", "year",
                        1981, "count", 5), ImmutableMap.of("target",
                        "@foo:ce4ad6a1cd6293d9", "hotness", "WARM", "year",
                        1980, "count", 3), ImmutableMap.of("target",
                        "@foo:ce4ad6a1cd6293d9", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 3), ImmutableMap.of("target",
                        "@foo:deab185c6138b16e", "hotness", "COOL", "year",
                        1981, "count", 5), ImmutableMap.of("target",
                        "@foo:deab185c6138b16e", "hotness", "WARM", "year",
                        1980, "count", 3), ImmutableMap.of("target",
                        "@foo:deab185c6138b16e", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 3), ImmutableMap.of("target",
                        "@foo:f79fe6c8ee441b18", "hotness", "COOL", "year",
                        1981, "count", 5), ImmutableMap.of("target",
                        "@foo:f79fe6c8ee441b18", "hotness", "WARM", "year",
                        1980, "count", 2), ImmutableMap.of("target",
                        "@foo:f79fe6c8ee441b18", "hotness", "TEH_HOTNESS",
                        "year", 1980, "count", 3)));

                List<Map<String, Object>> result04 = counts.doCounterQuery(
                        database, "complex", "byTargetHotnessYear",
                        ImmutableList.<QueryTerm> of(new QueryTerm(
                                QueryOperator.EQ, "target", new QueryValue(
                                        ValueType.STRING,
                                        "\"@foo:deab185c6138b16e\""))), null,
                        25L, def);

                jsoneq(result04, ImmutableList.of(ImmutableMap.of("hotness",
                        "COOL", "year", 1981, "count", 5), ImmutableMap.of(
                        "hotness", "WARM", "year", 1980, "count", 3),
                        ImmutableMap.of("hotness", "TEH_HOTNESS", "year", 1980,
                                "count", 3)));

                List<Map<String, Object>> result05 = counts.doCounterQuery(
                        database, "complex", "byTargetHotnessYear",
                        ImmutableList.<QueryTerm> of(new QueryTerm(
                                QueryOperator.EQ, "target", new QueryValue(
                                        ValueType.STRING,
                                        "\"@foo:deab185c6138b16e\"")),
                                new QueryTerm(QueryOperator.EQ, "hotness",
                                        new QueryValue(ValueType.STRING,
                                                "\"TEH_HOTNESS\""))), null,
                        25L, def);

                jsoneq(result05, ImmutableList.of(ImmutableMap.of("year", 1980,
                        "count", 3)));
            }
        });
    }

    protected void runCounterTest(String type, String counterName,
            String schema, SeedData provider, CounterAssertions assertions)
            throws Exception {
        this.schemaResource.createEntity(type, schema);
        Assert.assertTrue(this.counts.tableExists(database, type, counterName));

        for (int i = 0; i < 74; i++) {
            Map<String, Object> entity = provider.getData(i);
            String entityJson = EncodingHelper.convertToJson(entity);

            this.kvResource.createEntity(type, entityJson).getEntity();
        }

        assertions.assertCounters(mapper.readValue(schema,
                SchemaDefinition.class));

        for (int i = 0; i < 74; i++) {
            Map<String, Object> entity = provider.getData(i);
            Map<String, Object> actual = new LinkedHashMap<String, Object>();
            actual.put("version", "1");
            actual.putAll(entity);

            String entityJson = EncodingHelper.convertToJson(actual);

            this.kvResource.updateEntity(
                    new Key(type, (long) i).getEncryptedIdentifier(),
                    entityJson);
        }

        for (int i = 0; i < 74; i++) {
            this.kvResource.deleteEntity(new Key(type, (long) i)
                    .getEncryptedIdentifier());
        }
    }

    protected void jsoneq(Object a, Object b) throws Exception {
        Assert.assertEquals(mapper.writeValueAsString(a),
                mapper.writeValueAsString(b));
    }

    public interface SeedData {
        public Map<String, Object> getData(long seed);
    }

    public interface CounterAssertions {
        public void assertCounters(SchemaDefinition def) throws Exception;
    }
}