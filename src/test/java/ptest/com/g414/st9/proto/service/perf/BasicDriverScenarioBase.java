package ptest.com.g414.st9.proto.service.perf;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import ptest.com.g414.st9.proto.service.KeyValueOperations;

import com.g414.dgen.EntityGenerator;
import com.g414.dgen.range.LongRangeBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.TypeLiteral;
import com.sun.faban.driver.HttpClientFacade;
import com.sun.faban.driver.transport.sunhttp.SunHttpTransportClientFacade;

public class BasicDriverScenarioBase {
    protected HttpClientFacade http;

    @Inject
    protected Iterator<Long> createRange;

    protected Random random = new Random();

    protected final EntityGenerator entities = BasicEntity.createGenerator();
    protected final String host = System.getProperty("http.host", "localhost");
    protected final int port = Integer.parseInt(System.getProperty("http.port",
            "8080"));

    protected volatile long maxId = 0L;

    public BasicDriverScenarioBase() throws Exception {
        http = (HttpClientFacade) SunHttpTransportClientFacade.newInstance();
    }

    public void create() throws Exception {
        Long nextId = createRange.next();

        Map<String, Object> entity = entities.getEntity(nextId.toString());

        KeyValueOperations.create(host, port, http, (String) entity.get("id"),
                entity);

        this.maxId = Math.max(0, nextId - 500);
    }

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
        }
    }
}
