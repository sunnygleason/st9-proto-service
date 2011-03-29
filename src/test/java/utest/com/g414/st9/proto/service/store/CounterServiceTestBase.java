package utest.com.g414.st9.proto.service.store;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.KeyValueResource;
import com.g414.st9.proto.service.ServiceModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.store.CounterService;
import com.g414.st9.proto.service.store.Key;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

@Test
public abstract class CounterServiceTestBase {
    protected KeyValueResource kvResource;
    protected CounterService counters;

    public abstract Module getStorageModule();

    public CounterServiceTestBase() {
        Injector injector = Guice.createInjector(new LifecycleModule(),
                getStorageModule(), new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(KeyValueCache.class).toInstance(
                                new EmptyKeyValueCache());
                    }
                }, new ServiceModule());

        this.kvResource = injector.getInstance(KeyValueResource.class);
        this.counters = injector.getInstance(CounterService.class);

        injector.getInstance(Lifecycle.class).init();
        injector.getInstance(Lifecycle.class).start();
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        this.kvResource.clear();
        this.counters.clear();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        this.kvResource.clear();
        this.counters.clear();
    }

    public void testCounterService() throws Exception {
        assertEquals(counters.nextKey("foo"), Key.valueOf("foo:1"));
        assertEquals(counters.getTypeId("foo"), Integer.valueOf(2));

        for (int i = 2; i < 100000; i++) {
            assertEquals(counters.peekKey("foo"), Key.valueOf("foo:" + i));
            assertEquals(counters.nextKey("foo"), Key.valueOf("foo:" + i));
        }

        assertEquals(counters.peekKey("foo"), null);
        assertEquals(counters.nextKey("foo"), Key.valueOf("foo:100001"));

        assertEquals(counters.nextKey("bar"), Key.valueOf("bar:1"));
        assertEquals(counters.getTypeId("bar"), Integer.valueOf(3));
    }
}