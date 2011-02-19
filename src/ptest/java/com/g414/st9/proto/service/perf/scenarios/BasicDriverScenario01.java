package com.g414.st9.proto.service.perf.scenarios;

import static com.sun.faban.driver.CycleType.THINKTIME;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.g414.dgen.EntityGenerator;
import com.g414.dgen.range.LongRangeBuilder;
import com.g414.st9.proto.service.KeyValueOperations;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.sun.faban.driver.BenchmarkDefinition;
import com.sun.faban.driver.BenchmarkDriver;
import com.sun.faban.driver.BenchmarkOperation;
import com.sun.faban.driver.FlatMix;
import com.sun.faban.driver.HttpClientFacade;
import com.sun.faban.driver.NegativeExponential;
import com.sun.faban.driver.transport.sunhttp.SunHttpTransportClientFacade;

@BenchmarkDefinition(name = "BasicDriverScenario01", version = "1.0")
@BenchmarkDriver(name = "BasicDriverScenario01", responseTimeUnit = TimeUnit.MICROSECONDS)
@FlatMix(operations = { "create", "update", "retrieve" }, mix = { 0.1, 0.05,
        0.85 })
public class BasicDriverScenario01 {
    private HttpClientFacade http;

    @Inject
    private Iterator<Long> createRange;

    private Random random = new Random();

    private final EntityGenerator entities = BasicEntity.createGenerator();
    private final String host = "localhost";
    private final int port = 8080;

    private volatile long maxId = 0L;

    public BasicDriverScenario01() throws Exception {
        http = (HttpClientFacade) SunHttpTransportClientFacade.newInstance();
    }

    @BenchmarkOperation(name = "create", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void create() throws Exception {
        Long nextId = createRange.next();

        Map<String, Object> entity = entities.getEntity(nextId.toString());

        KeyValueOperations.create(host, port, http, (String) entity.get("id"),
                entity);

        this.maxId = Math.max(0, nextId - 500);
    }

    @BenchmarkOperation(name = "update", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void update() throws Exception {
        if (maxId == 0) {
            create();

            return;
        }

        Long nextId = (long) random.nextInt((int) maxId);
        Map<String, Object> entity = entities.getEntity(nextId.toString());
        KeyValueOperations.update(host, port, http, (String) entity.get("id"),
                entity);
    }

    @BenchmarkOperation(name = "retrieve", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void retrieve() throws Exception {
        if (maxId == 0) {
            create();

            return;
        }

        Long nextId = (long) random.nextInt((int) maxId);
        Map<String, Object> entity = entities.getEntity(nextId.toString());

        KeyValueOperations
                .retrieve(host, port, http, (String) entity.get("id"));
    }

    public static class GuiceModule extends AbstractModule {
        @Override
        protected void configure() {
            long min = Long.parseLong(System.getProperty("min", "1"));
            long max = Long.parseLong(System.getProperty("max", "20000000"));

            Iterator<Long> longIter = (new LongRangeBuilder(min, max))
                    .setRepeating(false).build().iterator();

            bind(new TypeLiteral<Iterator<Long>>() {
            }).toInstance(longIter);

            bind(Key.get(Object.class, BenchmarkDriver.class)).to(
                    BasicDriverScenario01.class);
        }
    }
}
